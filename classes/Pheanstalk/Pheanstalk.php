<?php

/**
 * Pheanstalk is a pure PHP 5.2+ client for the beanstalkd workqueue.
 * The Pheanstalk class is a simple facade for the various underlying components.
 *
 * @see http://github.com/kr/beanstalkd
 * @see http://xph.us/software/beanstalkd/
 *
 * @author Paul Annesley
 * @package Pheanstalk
 * @licence http://www.opensource.org/licenses/mit-license.php
 */
class Pheanstalk_Pheanstalk implements Pheanstalk_PheanstalkInterface
{
    const VERSION = '2.0.0';

    private $_connections = array();
    private $_using = Pheanstalk_PheanstalkInterface::DEFAULT_TUBE;
    private $_watching = array(Pheanstalk_PheanstalkInterface::DEFAULT_TUBE => true);
    
    /**
     * @param string $host
     * @param int $port
     * @param int $connectTimeout
     */
    public function __construct($host = null, $port = Pheanstalk_PheanstalkInterface::DEFAULT_PORT, $connectTimeout = null)
    {
        if(!is_null($host)) {
            $this->addConnection(new Pheanstalk_Connection($host, $port, $connectTimeout));
        }
    }

    /**
     * {@inheritDoc}
     */
    public function addConnection(Pheanstalk_Connection $connection)
    {
        $name = $connection->getName();
        $this->_connections[$name] = $connection;
        
        return $name;
    }

    /**
     * {@inheritDoc}
     */
    public function getConnections()
    {
        return $this->_connections;
    }
    
    public function getConnection($name)
    {
        if(isset($this->_connections[$name]))
            return $this->_connections[$name];
        else
            throw new Pheanstalk_Exception('Unknown connection: '.$name);
    }


    // ----------------------------------------

    /**
     * {@inheritDoc}
     */
    public function bury(Pheanstalk_Job $job, $priority = Pheanstalk_PheanstalkInterface::DEFAULT_PRIORITY)
    {
        $this->_dispatch(new Pheanstalk_Command_BuryCommand($job, $priority), $job->getConnection());
    }

    /**
     * {@inheritDoc}
     */
    public function delete(Pheanstalk_Job $job)
    {
        $response = $this->_dispatch(new Pheanstalk_Command_DeleteCommand($job), $job->getConnection());
        return $this;
    }

    /**
     * {@inheritDoc}
     */
    public function ignore($tube)
    {
        if (isset($this->_watching[$tube])) {
            unset($this->_watching[$tube]);
            $connections = $this->getConnections();
            foreach($connections as $connection) {
                try {
                    $this->_dispatch(new Pheanstalk_Command_IgnoreCommand($tube), $connection);
                } catch (Pheanstalk_Exception_SocketException $e) {
                    
                }
            }
        }
        return $this;
    }

    /**
     * {@inheritDoc}
     */
    public function kick($max)
    {
        $kicked = 0;
        $connections = $this->getConnections();
        foreach($connections as $connection) {
            try {
                $response = $this->_dispatch(new Pheanstalk_Command_KickCommand($max), $connection);
                $kicked += $response['kicked'];
            } catch (Pheanstalk_Exception_SocketException $e) {
                
            }
        }
        return $kicked;
    }

    /**
     * {@inheritDoc}
     */
    public function kickJob(Pheanstalk_Job $job)
    {
        $this->_dispatch(new Pheanstalk_Command_KickJobCommand($job), $job->getConnection());
        return $this;
    }

    /**
     * {@inheritDoc}
     */
    public function listTubes()
    {
        $list = array();
        
        $connections = $this->getConnections();
        foreach($connections as $name => $connection) {
            try {
                $list = array_merge($list, (array) $this->_dispatch(new Pheanstalk_Command_ListTubesCommand(), $connection));
            } catch (Pheanstalk_Exception_SocketException $e) {
                
            }
        }

        return $list;
    }

    /**
     * {@inheritDoc}
     */
    public function listTubesWatched($askServer = false)
    {
        if (!$askServer) {
            return array_keys($this->_watching);
        }
        
        $list = array();
        
        $connections = $this->getConnections();
        foreach($connections as $name => $connection) {
            try {
                $list[$name] = (array) $this->_dispatch(new Pheanstalk_Command_ListTubesWatchedCommand(), $connection);
            } catch (Pheanstalk_Exception_SocketException $e) {
                
            }
        }

        return $list;
    }

    /**
     * {@inheritDoc}
     */
    public function listTubeUsed($askServer = false)
    {
        if (!$askServer) {
            return $this->_using;
        }

        $list = array();
        
        $connections = $this->getConnections();
        foreach($connections as $name => $connection) {
            try {
                $response = $this->_dispatch(new Pheanstalk_Command_ListTubeUsedCommand(), $connection);
                $list[$name] = $response['tube'];
            } catch (Pheanstalk_Exception_SocketException $e) {
                
            }
            
        }
        
        return $list;

    }

    /**
     * {@inheritDoc}
     */
    public function pauseTube($tube, $delay)
    {
        $connections = $this->getConnections();
        foreach($connections as $name => $connection) {
            try {
                        $this->_dispatch(new Pheanstalk_Command_PauseTubeCommand($tube, $delay), $connection);
            } catch (Pheanstalk_Exception_SocketException $e) {
                
            }
        }
        
        return $this;
    }

    /**
     * {@inheritDoc}
     */
    public function peek($jobId, $connectionName)
    {
        $connection = $this->getConnection($connectionName);
        $response = $this->_dispatch(new Pheanstalk_Command_PeekCommand($jobId), $connection);

        return new Pheanstalk_Job($response['id'], $response['jobdata'], $connection);
    }

    /**
     * {@inheritDoc}
     */
    public function peekReady($tube = null, $connectionName = null)
    {
        if ($tube !== null) {
            $this->useTube($tube);
        }
        
        if(!is_null($connectionName))
            $connection = $this->_getConnection();
        else
            $connection = $this->getConnection($connectionName);

        $response = $this->_dispatch(new Pheanstalk_Command_PeekCommand(Pheanstalk_Command_PeekCommand::TYPE_READY), $connection);

        return new Pheanstalk_Job($response['id'], $response['jobdata'], $connection);
    }

    /**
     * {@inheritDoc}
     */
    public function peekDelayed($tube = null, $connectionName = null)
    {
        if ($tube !== null) {
            $this->useTube($tube);
        }

        if(is_null($connectionName))
            $connection = $this->_getConnection();
        else
            $connection = $this->getConnection($connectionName);

        $response = $this->_dispatch(new Pheanstalk_Command_PeekCommand(Pheanstalk_Command_PeekCommand::TYPE_DELAYED), $connection);

        return new Pheanstalk_Job($response['id'], $response['jobdata']);
    }

    /**
     * {@inheritDoc}
     */
    public function peekBuried($tube = null, $connectionName = null)
    {
        if ($tube !== null) {
            $this->useTube($tube);
        }

        if(is_null($connectionName))
            $connection = $this->_getConnection();
        else
            $connection = $this->getConnection($connectionName);

        $response = $this->_dispatch(new Pheanstalk_Command_PeekCommand(Pheanstalk_Command_PeekCommand::TYPE_BURIED), $connection);

        return new Pheanstalk_Job($response['id'], $response['jobdata']);
    }

    /**
     * {@inheritDoc}
     */
    public function put(
        $data,
        $priority = Pheanstalk_PheanstalkInterface::DEFAULT_PRIORITY,
        $delay = Pheanstalk_PheanstalkInterface::DEFAULT_DELAY,
        $ttr = Pheanstalk_PheanstalkInterface::DEFAULT_TTR
    )
    {

        while(true) {
            try {
                $connection = $this->_getConnection();
                $response = $this->_dispatch(new Pheanstalk_Command_PutCommand($data, $priority, $delay, $ttr), $connection);
                return $response['id'];
            } catch (Pheanstalk_Exception_SocketException $e) {
                continue;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    public function putInTube(
        $tube,
        $data,
        $priority = Pheanstalk_PheanstalkInterface::DEFAULT_PRIORITY,
        $delay = Pheanstalk_PheanstalkInterface::DEFAULT_DELAY,
        $ttr = Pheanstalk_PheanstalkInterface::DEFAULT_TTR
    )
    {
        $this->useTube($tube);

        return $this->put($data, $priority, $delay, $ttr);
    }

    /**
     * {@inheritDoc}
     */
    public function release(
        Pheanstalk_Job $job,
        $priority = Pheanstalk_PheanstalkInterface::DEFAULT_PRIORITY,
        $delay = Pheanstalk_PheanstalkInterface::DEFAULT_DELAY
    )
    {
        $response = $this->_dispatch(new Pheanstalk_Command_ReleaseCommand($job, $priority, $delay), $job->getConnection());

        return $this;
    }

    /**
     * {@inheritDoc}
     */
    public function reserve($timeout = null)
    {
        if(is_null($timeout))
            $timeout = PHP_INT_MAX;
        
        $jobs = $this->reserveMultiSync(0);

        
        while(count($jobs) < 1 AND $timeout--) {
            $jobs = $this->reserveMultiSync(1);
        }
        
        if(count($jobs) < 1)
            return false;
            
        $k = array_rand($jobs);
        $reservedJob = $jobs[$k];
        unset($jobs[$k]);

        foreach($jobs as $n => $job) {
            $this->release($job);
        }
        
        return $reservedJob;
    }
    
    public function reserveMultiSync($timeout)
    {
        $falseResponses = array(
            Pheanstalk_Response::RESPONSE_DEADLINE_SOON,
            Pheanstalk_Response::RESPONSE_TIMED_OUT,
        );
        $command = new Pheanstalk_Command_ReserveCommand($timeout);
        $jobs = array();
        $isAvaibleConnections = false;

        foreach($this->_connections as $name => $connection) {

            if(!$connection->isActive()) {
                if($connection->getNextReconnect() > time()) {
                    continue;
                } elseif(!$this->_reconnect($connection)) {
                    continue;
                }
            }

            try{
                $connection->sendCommand($command);
                $isAvaibleConnections = true;
            } catch (Pheanstalk_Exception_SocketException $e) {
                $connection->setInactive();
            }
        }
        
        if(!$isAvaibleConnections)
            throw new Pheanstalk_Exception('All connections down');
        
        foreach($this->_connections as $name => $connection) {
            if(!$connection->isActive()) {
                continue;
            }
            
            try{
                $response = $connection->readResponse($command);
            } catch (Pheanstalk_Exception_SocketException $e) {
                $connection->setInactive();
                continue;
            }

            if(!in_array($response->getResponseName(), $falseResponses)) {
                $jobs[] = new Pheanstalk_Job($response['id'], $response['jobdata'], $connection);
            }
        }
        
        return $jobs;
    }

    /**
     * {@inheritDoc}
     */
    public function reserveFromTube($tube, $timeout = null)
    {
        $this->watchOnly($tube);
        return $this->reserve($timeout);
    }

    /**
     * {@inheritDoc}
     */
    public function statsJob(Pheanstalk_Job $job)
    {
        return $this->_dispatch(new Pheanstalk_Command_StatsJobCommand($job), $job->getConnection());
    }

    /**
     * {@inheritDoc}
     */
    public function statsTube($tube, $connectionName = null)
    {
        if(!is_null($connectionName))
            return $this->_dispatch(new Pheanstalk_Command_StatsTubeCommand($tube), $this->getConnection($connectionName));
        
        $stats = array();
        $connections = $this->getConnections();
        foreach($connections as $name => $connection) {
            try {
                $stats[$name] = $this->_dispatch(new Pheanstalk_Command_StatsTubeCommand($tube), $connection);
            } catch (Pheanstalk_Exception_SocketException $e) {
                $stats[$name] = false;
            }
        }

        return $stats;

    }

    public function statsTubeSummary($tube)
    {
        
        $summary = array();
        $connections = $this->getConnections();
        foreach($connections as $name => $connection) {
            $stats = $this->statsTube($tube, $connection->getName());
            
            foreach($stats as $name => $value) {
                if(!isset($summary[$name]))
                    $summary[$name] = 0;
                
                $summary[$name] += $value;
            }
        }

        return $summary;

    }

    /**
     * {@inheritDoc}
     */
    public function stats($connectionName = null)
    {
        if(!is_null($connectionName))
            return $this->_dispatch(new Pheanstalk_Command_StatsCommand(), $this->getConnection($connectionName));

        $stats = array();
        $connections = $this->getConnections();
        foreach($connections as $name => $connection) {
            try {
                $stats[$name] = $this->_dispatch(new Pheanstalk_Command_StatsCommand(), $connection);
            } catch (Pheanstalk_Exception_SocketException $e) {
                $stats[$name] = false;
            }
                
        }

        return $stats;
    }

    /**
     * {@inheritDoc}
     */
    public function touch(Pheanstalk_Job $job)
    {
        $this->_dispatch(new Pheanstalk_Command_TouchCommand($job), $job->getConnection());
        return $this;
    }

    /**
     * {@inheritDoc}
     */
    public function useTube($tube)
    {
        if ($this->_using != $tube) {
            $this->_using = $tube;
            $connections = $this->getConnections();
            foreach($connections as $name => $connection) {
                try {
                    $this->_dispatch(new Pheanstalk_Command_UseCommand($tube), $connection);
                } catch (Pheanstalk_Exception_SocketException $e) {
                    
                }
            }
        }
        return $this;
    }

    /**
     * {@inheritDoc}
     */
    public function watch($tube)
    {
        if (!isset($this->_watching[$tube])) {
            $this->_watching[$tube] = true;
            $connections = $this->getConnections();
            foreach($connections as $name => $connection) {
                try {
                    $this->_dispatch(new Pheanstalk_Command_WatchCommand($tube), $connection);
                } catch (Pheanstalk_Exception_SocketException $e) {

                }
            }
        }
        return $this;
    }

    /**
     * {@inheritDoc}
     */
    public function watchOnly($tube)
    {
        $this->watch($tube);

        $ignoreTubes = array_diff_key($this->_watching, array($tube => true));
        foreach ($ignoreTubes as $ignoreTube => $true) {
            $this->ignore($ignoreTube);
        }

        return $this;
    }

    // ----------------------------------------


    /**
     * {@inheritDoc}
     */
    private function _getConnection()
    {
        $keys = array_keys($this->_connections);
        shuffle($keys);
//        DebugBreak('1@localhost');
        foreach($keys as $key) {
            $connection = $this->_connections[$key];
            
            // if connection is not actived, try to reconnect every 10 sec
            if(!$connection->isActive()) {
                if($connection->getNextReconnect() > time()) {
                    continue;
                } elseif(!$this->_reconnect($connection)) {
                    continue;
                }
            }
            return $connection;
        }
        
        throw new Pheanstalk_Exception('All connections down');
    }


    /**
     * Dispatches the specified command to the connection object.
     *
     * If a SocketException occurs, the connection is reset, and the command is
     * re-attempted once.
     *
     * @param Pheanstalk_Command $command
     * @return Pheanstalk_Response
     */
    private function _dispatch(Pheanstalk_Command $command, Pheanstalk_Connection $connection)
    {
        if(!$connection->isActive()) {
            
        }
        
        try {
            $response = $connection->dispatchCommand($command);
        } catch (Pheanstalk_Exception_SocketException $e) {
            if($this->_reconnect($connection)) {
                $response = $connection->dispatchCommand($command);
            } else {
                $connection->setInactive();
                throw $e;
            }
        }
        return $response;
    }
    
    /**
     * Creates a new connection object, based on the existing connection object,
     * and re-establishes the used tube and watchlist.
     */
    private function _reconnect(Pheanstalk_Connection $connection)
    {
        if(!$connection->reconnect())
            return false;

        if ($this->_using != Pheanstalk_PheanstalkInterface::DEFAULT_TUBE) {
            $connection->dispatchCommand(new Pheanstalk_Command_UseCommand($this->_using));
        }

        foreach ($this->_watching as $tube => $true) {
            if ($tube != Pheanstalk_PheanstalkInterface::DEFAULT_TUBE) {
                $connection->dispatchCommand(new Pheanstalk_Command_WatchCommand($tube));
            }
        }

        if (!isset($this->_watching[Pheanstalk_PheanstalkInterface::DEFAULT_TUBE])) {
            $connection->dispatchCommand(new Pheanstalk_Command_IgnoreCommand(Pheanstalk_PheanstalkInterface::DEFAULT_TUBE));
        }
        
        return true;
    }
}
