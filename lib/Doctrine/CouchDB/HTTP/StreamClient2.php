<?php
/*
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * and is licensed under the MIT license. For more information, see
 * <http://www.doctrine-project.org>.
 */

namespace Doctrine\CouchDB\HTTP;
use Doctrine\CouchDB\CouchDBClient;

/**
 * Connection handler using PHPs stream wrappers.
 *
 * Requires PHP being compiled with --with-curlwrappers for now, since the PHPs
 * own HTTP implementation is somehow b0rked.
 *
 * @license     http://www.opensource.org/licenses/lgpl-license.php LGPL
 * @link        www.doctrine-project.com
 * @since       1.0
 * @author      Kore Nordmann <kore@arbitracker.org>
 */
class StreamClient2 extends AbstractHTTPClient
{
    protected $httpFilePointer;

    protected function getHeaders()
    {
        $headers = array();
        if($this->httpFilePointer !== false) {

            $metaData = stream_get_meta_data($this->httpFilePointer);
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
     * Perform a request to the server and return the result
     *
     * Perform a request to the server and return the result converted into a
     * Response object. If you do not expect a JSON structure, which
     * could be converted in such a response object, set the forth parameter to
     * true, and you get a response object retuerned, containing the raw body.
     *
     * @param string $method
     * @param string $path
     * @param string $data
     * @param bool $raw
     * @return Response
     * @throws HTTPException
     */
    public function request( $method, $path, $data = null, $raw = false, $target = null , $docId = null )
    {
        $basicAuth = '';
        if ( $this->options['username'] ) {
            $basicAuth .= "{$this->options['username']}:{$this->options['password']}@";
        }

        // TODO SSL support?
        $this->httpFilePointer = @fopen(
            'http://' . $basicAuth . $this->options['host']  . ':' . $this->options['port'] . $path,
            'r',
            false,
            stream_context_create(
                array(
                    'http' => array(
                        'method'        => $method,
                        'content'       => json_encode($data),
                        'ignore_errors' => true,
                        'max_redirects' => 0,
                        'user_agent'    => 'Doctrine CouchDB ODM $Revision$',
                        'timeout'       => $this->options['timeout'],
                        'header'        => 'Content-type: application/json',
                    ),
                )
            )
        );

        // Check if connection has been established successfully
        if ( $this->httpFilePointer === false ) {
            $error = error_get_last();
            throw HTTPException::connectionFailure(
                $this->options['ip'],
                $this->options['port'],
                $error['message'],
                0
            );
        }

        // Read request body
        $body = $this->parseAndSend($target, $docId);//returns an array of docs without attachement


        $headers = $this->getHeaders();

        if ( empty($headers['status']) ) {
            throw HTTPException::readFailure(
                $this->options['ip'],
                $this->options['port'],
                'Received an empty response or not status code',
                0
            );
        }

        // Create repsonse object from couch db response
        if ( $headers['status'] >= 400 )
        {
            return new ErrorResponse( $headers['status'], $headers, $body );
        }
        return new Response( $headers['status'], $headers, $body, true );
    }

    protected function getLinesFromStream()
    {
        while ( !feof( $this->httpFilePointer ) ) {
            $line = fgets( $this->httpFilePointer );
            yield $line;
        }
    }

    protected function getNextLine(\Generator & $lines)
    {
        $line = $lines->current();
        $lines->next();
        return $line;
    }

    protected function parseAndSend(CouchDBClient $target, $docId)
    {
        $lines = $this->getLinesFromStream();
      /*  foreach($lines as $line) {
            echo $line;
        }
        return '';*/

        $mainBoundary = trim($this->getNextLine($lines));
        $docStack = array();
        //echo "mainb= ".$mainBoundary."\n";

        while ($lines->valid()) {
            $line = ltrim($this->getNextLine($lines));
            //echo "here line=". $line."\n";
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
                        //missing revs at source
                        //continue till the end of this document
                        while (strpos($this->getNextLine($lines), $mainBoundary) === false) ;

                    } else {
                        throw new \Exception("Unknown parameter with Content-Type.");
                    }

                }
                if (strpos($value, "multipart/related") !==false) {

                    if ($boundary == '') {
                        throw new \Exception("Boundary not set for multipart/related data.");
                    }
                    $boundary = explode("=",$boundary,2)[1];

                    //echo "line==".$line;
                    $path = '/' . $target->getDatabase() . '/' . urlencode($docId) . "?new_edits=false";
                    $res = $target->getHttpClient()->sendStream('PUT', $path, $lines, $mainBoundary, false,array
                    ('Content-Type' =>
                        'multipart/related; boundary=' . $boundary
                    ));

                } elseif ($value == 'application/json') {
                    $jsonDoc = '';
                    while(trim(($jsonDoc = $this->getNextLine($lines))) == '');
                    array_push($docStack, trim($jsonDoc));
                    //var_dump($jsonDoc);
                    //continue till the end of this document
                    while (strpos($this->getNextLine($lines), $mainBoundary) === false) ;

                } else {
                    throw new \UnexpectedValueException("This value is not supported.");
                }
            } else {
                throw new \Exception('The first line is not the Content-Type.');
            }
            $lines->next();
        }
        return $docStack;
    }


}

