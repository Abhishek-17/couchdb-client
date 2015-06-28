<?php


namespace Doctrine\CouchDB\HTTP;


/**
 * A custom HTTP client which streams data from the source to
 * the target and thus does the transfer with lesser memory
 * footprint.
 *
 * Class MultipartClient
 * @package Doctrine\CouchDB\HTTP
 */
class MultipartClient extends AbstractHTTPClient
{
    /**
     * @var array
     */
    protected $sourceOptions;
    /**
     * @var null
     */
    protected $sourceConnection;
    /**
     * @var array
     */
    protected $targetOptions;
    /**
     * @var null
     */
    protected $targetConnection;


    /**
     * @param AbstractHTTPClient $source
     * @param AbstractHTTPClient $target
     */
    public function __construct(AbstractHTTPClient $source, AbstractHTTPClient $target)
    {
        $this->sourceOptions = $source->options;
        $this->targetOptions = $target->options;
        $this->sourceConnection = null;
        $this->targetConnection = null;

    }

    /**
     * @param $method
     * @param $sourcePath
     * @param $sourceHeaders
     * @param $data
     * @throws HTTPException
     */
    protected function checkSourceConnection($method, $sourcePath, $sourceHeaders, & $data)
    {
        //setup stream connection to the source.
        $basicAuth = '';
        if ( $this->sourceOptions['username'] ) {
            $basicAuth .= "{$this->sourceOptions['username']}:{$this->sourceOptions['password']}@";
        }
        if (!isset($sourceHeaders['Content-Type'])) {
            $sourceHeaders['Content-Type'] = 'application/json';
        }
        $header = '';
        if ($sourceHeaders != null) {
            foreach ($sourceHeaders as $key => $val) {
                $header .= $key . ": " . $val . "\r\n";
            }
        }

        // TODO SSL support?
        $this->sourceConnection = ($this->sourceConnection ? $this->sourceConnection : @fopen(
            'http://' . $basicAuth . $this->sourceOptions['host']  . ':' . $this->sourceOptions['port'] . $sourcePath,
            'r',
            false,
            stream_context_create(
                array(
                    'http' => array(
                        'method'        => $method,
                        'content'       => $data,
                        'ignore_errors' => true,
                        'max_redirects' => 0,
                        'user_agent'    => 'Doctrine CouchDB ODM $Revision$',
                        'timeout'       => $this->sourceOptions['timeout'],
                        'header'        => $header,
                    ),
                )
            )
        ) );

        // Check if connection has been established successfully
        if ( $this->sourceConnection === false ) {
            $error = error_get_last();
            throw HTTPException::connectionFailure(
                $this->sourceOptions['ip'],
                $this->sourceOptions['port'],
                $error['message'],
                0
            );
        }
    }

    /**
     * @throws HTTPException
     */
    protected function  checkTargetConnection()
    {
        //setup connection to the target

        // Setting Connection scheme according ssl support
        if ($this->targetOptions['ssl']) {
            if (!extension_loaded('openssl')) {
                // no openssl extension loaded.
                // This is a bit hackisch...
                $this->targetConnection = null;

                throw HTTPException::connectionFailure(
                    $this->targetOptions['ip'],
                    $this->targetOptions['port'],
                    "ssl activated without openssl extension loaded",
                    0
                );
            }

            $host = 'ssl://' . $this->targetOptions['host'];

        } else {
            $host = $this->targetOptions['ip'];
        }

        // $errno = '';
        // $errstr = '';
        // If the connection could not be established, fsockopen sadly does not
        // only return false (as documented), but also always issues a warning.
        if ( ( $this->targetConnection === null ) &&
            ( ( $this->targetConnection = @fsockopen($host, $this->targetOptions['port'], $errno,
                    $errstr)
                ) === false ) )
        {
            // This is a bit hackisch...
            $this->targetConnection = null;
            throw HTTPException::connectionFailure(
                $this->targetOptions['ip'],
                $this->targetOptions['port'],
                $errstr,
                $errno
            );
        }
    }

    /**
     * @param $connection
     * @return array
     */
    protected function getStreamHeaders($connection)
    {
        $headers = array();
        if($connection !== false) {

            $metaData = stream_get_meta_data($connection);
            // The structure of this array differs depending on PHP compiled with
            // --enable-curlwrappers or not. Both cases are normally required.
            $rawHeaders = isset($metaData['wrapper_data']['headers'])
                ? $metaData['wrapper_data']['headers'] : $metaData['wrapper_data'];

            foreach ($rawHeaders as $lineContent) {
                // Extract header values
                if (preg_match('(^HTTP/(?P<version>\d+\.\d+)\s+(?P<status>\d+))S', $lineContent, $match)) {
                    $headers['version'] = $match['version'];
                    $headers['status'] = (int)$match['status'];
                } else {
                    list($key, $value) = explode(':', $lineContent, 2);
                    $headers[strtolower($key)] = ltrim($value);
                }
            }
        }
        return $headers;
    }

    /**
     * @param string $method
     * @param string $sourcePath
     * @param null $data
     * @param bool $raw
     * @param array $sourceRequestHeaders
     * @param string $targetPath
     * @return ErrorResponse
     * @throws HTTPException
     * @throws \Exception
     */
    public function request(
        $method,
        $sourcePath,
        $data = null,
        $raw = false,
        $sourceRequestHeaders = array(),
        $targetPath = '/'
    ) {
        $this->checkSourceConnection($method, $sourcePath, $sourceRequestHeaders, $data);
        $sourceResponseHeaders = $this->getStreamHeaders($this->sourceConnection);

        //an array containing
        //1) array of json docs(string) that don't have attachements.
        //2) responses of posting docs with attachements
        $body = '';

        if(isset($sourceResponseHeaders['status']) && $sourceResponseHeaders['status'] == 200) {
            $body = $this->parseAndSend($targetPath);
        } else {
            while (!feof($this->sourceConnection)) {
                $body .= fgets($this->sourceConnection);
            }
        }

        if (empty($sourceResponseHeaders['status'])) {
            throw HTTPException::readFailure(
                $this->sourceOptions['ip'],
                $this->sourceOptions['port'],
                'Received an empty response or not status code',
                0
            );
        } elseif ($sourceResponseHeaders['status'] >= 400) {
            return new ErrorResponse($sourceResponseHeaders['status'], $sourceResponseHeaders, $body);
        } else {
            if ($sourceResponseHeaders['status'] == 200) {
                $sourceResponseHeaders['status'] = 201;
            }
            return new Response($sourceResponseHeaders['status'], $sourceResponseHeaders, $body, true);
        }


    }

    /**
     * @param $method
     * @param $path
     * @param $headers
     * @return string
     */
    protected function createHeader($method, $path, $headers)
    {
        // Create basic request headers
        $request = "$method $path HTTP/1.1\r\nHost: {$this->options['host']}\r\n";
        // Add basic auth if set
        if ( $this->options['username'] )
        {
            $request .= sprintf( "Authorization: Basic %s\r\n",
                base64_encode( $this->options['username'] . ':' . $this->options['password'] )
            );
        }
        // Set keep-alive header, which helps to keep to connection
        // initilization costs low, especially when the database server is not
        // available in the locale net.
        $request .= "Connection: " . ( $this->options['keep-alive'] ? 'Keep-Alive' : 'Close' ) . "\r\n";
        if (!isset($headers['Content-Type'])) {
            $headers['Content-Type'] = 'application/json';
        }
        foreach ($headers as $key => $value) {
            if (is_bool($value) === true) {
                $value = ($value) ? 'true': 'false';
            }
            $request .=$key. ": ". $value. "\r\n";
        }
        return $request;
    }

    /**
     * @param $method
     * @param $path
     * @param $streamEnd
     * @param array $requestHeaders
     * @return string
     * @throws \HTTPException
     */
    public function sendStream($method, $path, $streamEnd, $requestHeaders = array())
    {
        $dataStream = $this->sourceConnection;
        $this->checkTargetConnection();

        //Store data till the start of the attachment to find out the Content-Length.
        //At present CouchDB can't handle chunked data and needs Content-Length header.
        $str = '';
        $jsonFlag = 0;
        $attachmentCount = 0;
        $totalAttachmentLength = 0;
        $streamLine = $this->getNextLineFromSourceConnection();
        while (
            $jsonFlag == 0 ||
            ($jsonFlag == 1 &&
                trim($streamLine) == ''
            )
        ) {
            $str .= $streamLine;
            if (strpos($streamLine,"Content-Type: application/json") !== false) {
                $jsonFlag = 1;
            }
            $streamLine = $this->getNextLineFromSourceConnection();
        }
        $docBoundaryLength = strlen(explode("=",$requestHeaders['Content-Type'],2)[1])+2;
        $json = json_decode($streamLine, true);

        foreach ($json['_attachments'] as $docName => $metaData) {
            //quotes and a "/r/n"
            $totalAttachmentLength += strlen('Content-Disposition: attachment; filename=') + strlen($docName) + 4;
            $totalAttachmentLength += strlen('Content-Type: ') + strlen($metaData["content_type"]) + 2;
            $totalAttachmentLength +=  strlen('Content-Length: ');
            if (isset($metaData["encoding"])) {
                $totalAttachmentLength += $metaData["encoded_length"] + strlen($metaData["encoded_length"]) + 2;
                $totalAttachmentLength += strlen('Content-Encoding: ') + strlen($metaData["encoding"]) + 2;
            } else {
                $totalAttachmentLength += $metaData["length"] + strlen($metaData["length"]) + 2;
            }
            $totalAttachmentLength += 2;
            $attachmentCount++;
        }

        $requestHeaders['Content-Length'] = strlen($str) + strlen($streamLine)
            + $totalAttachmentLength + $attachmentCount * (2 + $docBoundaryLength) + $docBoundaryLength + 2;

        $request = $this->createHeader($method, $path, $requestHeaders);
        $request .= "\r\n";
        $request .= $str;

        if ( fwrite( $this->targetConnection, $request ) === false ) {
            // Reestablish which seems to have been aborted
            //
            // The recursion in this method might be problematic if the
            // connection establishing mechanism does not correctly throw an
            // exception on failure.
            $this->targetConnection = null;
            return '';
            //return $this->sensendStream($method, $path, $streamEnd, $requestHeaders);
        }

        // Read server response headers
        $rawHeaders = '';
        $headers = array(
            'connection' => ( $this->options['keep-alive'] ? 'Keep-Alive' : 'Close' ),
        );

        while(!feof($dataStream) &&
            ($streamEnd === null ||
                strpos($streamLine, $streamEnd) ===
                false
            )
        ) {
            $totalSent = 0;
            $length = strlen($streamLine);
            while($totalSent != $length) {
                $sent = fwrite($this->targetConnection, substr($streamLine,$totalSent));
                if ($sent === false) {
                    throw new \HTTPException('Stream write error.');
                } else {
                    $totalSent += $sent;
                }
            }
            $streamLine = $this->getNextLineFromSourceConnection(10001);
        }


        // Remove leading newlines, should not accur at all, actually.
        while ( ( ( $line = fgets( $this->targetConnection ) ) !== false ) &&
            ( ( $lineContent = rtrim( $line ) ) === '' ) );

        // Throw exception, if connection has been aborted by the server, and
        // leave handling to the user for now.
        if ($line === false) {
              //sendStream can't be called in recursion as the stream can be
              //read only once.
             return '';
        }

        do {
            // Also store raw headers for later logging
            $rawHeaders .= $lineContent . "\n";
            // Extract header values
            if ( preg_match( '(^HTTP/(?P<version>\d+\.\d+)\s+(?P<status>\d+))S', $lineContent, $match ) )
            {
                $headers['version'] = $match['version'];
                $headers['status']  = (int) $match['status'];
            }
            else
            {
                list( $key, $value ) = explode( ':', $lineContent, 2 );
                $headers[strtolower( $key )] = ltrim( $value );
            }
        }  while ( ( ( $line = fgets( $this->targetConnection ) ) !== false ) &&
            ( ( $lineContent = rtrim( $line ) ) !== '' ) );


        // Read response body
        $body = '';

        // HTTP 1.1 supports chunked transfer encoding, if the according
        // header is not set, just read the specified amount of bytes.
        $bytesToRead = (int) ( isset( $headers['content-length'] ) ? $headers['content-length'] : 0 );
        // Read body only as specified by chunk sizes, everything else
        // are just footnotes, which are not relevant for us.
        while ($bytesToRead > 0) {
            $body .= $read = fgets( $this->targetConnection, $bytesToRead + 1 );
            $bytesToRead -= strlen( $read );
        }

        // Reset the connection if the server asks for it.
        if ($headers['connection'] !== 'Keep-Alive') {
            fclose( $this->targetConnection );
            $this->targetConnection = null;
        }
        // Handle some response state as special cases
        switch ($headers['status']) {
            case 301:
            case 302:
            case 303:
            case 307:
                $path = parse_url( $headers['location'], PHP_URL_PATH );
                //sendStream can't be called in recursion as the stream can be
                //read only once.
                //return $this->sensendStream($method, $path, $streamEnd, $requestHeaders);
        }

        return ($body != '' ? json_decode($body, true) : "Status Code:" . $headers['status']) ;
    }

    /**
     * @param int $maxLength
     * @return string
     */
    protected function getNextLineFromSourceConnection($maxLength = null)
    {
       if ($maxLength !== null) {
           return fgets($this->sourceConnection, $maxLength);
       } else {
           return fgets($this->sourceConnection);
       }
    }

    /**
     * @param $targetPath
     * @return array
     * @throws \Exception
     * @throws \HTTPException
     */
    protected function parseAndSend($targetPath)
    {

        $mainBoundary = trim($this->getNextLineFromSourceConnection());

        //Docs that don't have attachment.
        //These should be posted using bulk upload.
        $docStack = array();

        //Responses from posting docs that have attachments.
        $responses = array();

        while (!feof($this->sourceConnection)) {

            $line = ltrim($this->getNextLineFromSourceConnection());
            if($line == '')continue;
            if (strpos($line, "Content-Type") !== false) {
                list($header, $value) = explode(":", $line);
                $header = trim($header);
                $value = trim($value);
                $boundary = '';

                if (strpos($value, ";") !== false) {
                    list($type, $info) = explode(";", $value, 2);
                    $info = trim($info);

                    if (strpos($info, "boundary") !== false) {
                        $boundary = $info;

                    } elseif (strpos($info, "error") !== false) {

                        //missing revs at the source
                        //continue till the end of this document
                        while (strpos($this->getNextLineFromSourceConnection(), $mainBoundary) === false) ;

                    } else {
                        throw new \Exception("Unknown parameter with Content-Type.");
                    }

                }
                if (strpos($value, "multipart/related") !==false) {

                    if ($boundary == '') {
                        throw new \Exception("Boundary not set for multipart/related data.");
                    }
                    $boundary = explode("=",$boundary,2)[1];

                    try {
                        $responses[] = $this->sendStream(
                            'PUT',
                            $targetPath,
                            $mainBoundary,
                            array('Content-Type' => 'multipart/related; boundary=' . $boundary));
                    } catch (\Exception $e) {
                        $responses[] = $e;
                    }

                } elseif ($value == 'application/json') {
                    $jsonDoc = '';

                    while(trim(($jsonDoc = $this->getNextLineFromSourceConnection())) == '');
                    array_push($docStack, trim($jsonDoc));

                    //continue till the end of this document
                    while (strpos($this->getNextLineFromSourceConnection(), $mainBoundary) === false) ;

                } else {
                    throw new \UnexpectedValueException("This value is not supported.");
                }
            } else {
                throw new \Exception('The first line is not the Content-Type.');
            }
        }
        return array($docStack, $responses);
    }

}