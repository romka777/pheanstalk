<?php

/**
 * Wrapper around PHP stream functions.
 * Facilitates mocking/stubbing stream operations in unit tests.
 *
 * @author Paul Annesley
 * @package Pheanstalk
 * @licence http://www.opensource.org/licenses/mit-license.php
 */
class Pheanstalk_Socket_StreamFunctions
{
    private static $_instance;

    /**
     * Singleton accessor.
     */
    public static function instance()
    {
        if (empty(self::$_instance)) {
            self::$_instance = new self;
        }

        return self::$_instance;
    }

    /**
     * Sets an alternative or mocked instance.
     */
    public function setInstance($instance)
    {
        self::$_instance = $instance;
    }

    /**
     * Unsets the instance, so a new one will be created.
     */
    public function unsetInstance()
    {
        self::$_instance = null;
    }

    // ----------------------------------------

    public function fgets($handle, $length = 1048576)
    {
        return socket_read($handle, $length, PHP_NORMAL_READ);
    }

    public function fread($handle, $length)
    {
        return socket_read($handle, $length, PHP_BINARY_READ);
    }

    public function fsockopen($hostname, $port = -1, &$errno = null, &$errstr = null, $timeout = null)
    {
        $sock = @socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
        if(!$sock) {
            $errno = socket_last_error();
            $errstr = socket_strerror($errno);
            return false;
        }
        
        if(!socket_connect($sock, $hostname, $port)) {
            $errno = socket_last_error($sock);
            $errstr = socket_strerror($errno);
            return false;
        }
        
        return $sock;
    }

    public function fwrite($handle, $string, $length = null)
    {
        if (isset($length)) {
            return socket_write($handle, $string, $length);
        } else {
            return socket_write($handle, $string);
        }
    }

    public function stream_set_timeout($stream, $seconds, $microseconds = 0)
    {
        socket_set_option($stream, SOL_SOCKET, SO_RCVTIMEO, array('sec' => $seconds, 'usec' => $microseconds));
        socket_set_option($stream, SOL_SOCKET, SO_SNDTIMEO, array('sec'=> $seconds, 'usec' => $microseconds));
        
        return true;
    }
    
    public function stream_set_blocking($stream, $mode)
    {
        return stream_set_blocking($stream, $mode);
    }
    
    public function fclose($handle)
    {
        return socket_close($handle);
    }
    
    public function socket_select(&$read, &$write, &$except, $tv_sec, $tv_usec = 0)
    {
        return socket_select($read, $write, $except, $tv_sec, $tv_usec = 0);
    }
}
