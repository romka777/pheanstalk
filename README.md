Pheanstalk
==========

Fork of Pheanstalk.
Add multi-servers support.
Put places job on random server.
Reserve watch all servers to get job;


Usage Example
-------------

```php
<?php

// If you aren't using composer, register Pheanstalk class loader
require_once('pheanstalk_init.php');

$pheanstalk = new Pheanstalk_Pheanstalk();
$pheanstalk->addServer('127.0.0.1', 11300);
$pheanstalk->addServer('127.0.0.1', 11301);
$pheanstalk->addServer('127.0.0.1', 11302);

// ----------------------------------------
// producer (queues jobs)

$pheanstalk
  ->useTube('testtube')
  ->put("job payload goes here\n");

// ----------------------------------------
// worker (performs jobs)

$job = $pheanstalk
  ->watch('testtube')
  ->ignore('default')
  ->reserve();

echo $job->getData();

$pheanstalk->delete($job);


```


