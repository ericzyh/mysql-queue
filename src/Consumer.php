<?php

namespace MysqlQueue;

class Consumer
{
    private $pdo;

    private $getMessageStmt;

    private $updateMessageStmt;

    private $getMessageSql = "select id,queue_name,message,(add_time + delay) as consume_time
        from message_queue
        where queue_name = :queue_name and status = 0 having consume_time < unix_timestamp()
        order by priority desc, consume_time asc, id asc limit 1";

    private $updateMessageSql = "update message_queue set status = :status where id = :id";

    public function __construct($host, $port, $user, $password, $database)
    {
        try {
            $this->pdo = new \Pdo("mysql:host=$host;port=$port;dbname=$database", $user, $password, [\PDO::ATTR_PERSISTENT => true]);
            $this->getMessageStmt = $this->pdo->prepare($this->getMessageSql);
            $this->updateMessageStmt = $this->pdo->prepare($this->updateMessageSql);
        } catch (\Exception $e) {
            die("error in connecting to database");
        }
    }

    public function getPdo()
    {
        return $this->pdo;
    }

    public function consume($queueName, $callback, $sleep)
    {
        while (true) {
            try {
                $this->getMessageStmt->bindValue(":queue_name", $queueName);
                $this->getMessageStmt->execute();
                $results = $this->getMessageStmt->fetchAll();
                if ($results) {
                    foreach ($results as $result) {
                        $customExecute = call_user_func($callback, $result);
                        $this->updateMessageStmt->bindValue(":status", $customExecute ? 1 : 4);
                        $this->updateMessageStmt->bindValue(":id", $result['id']);
                        $this->updateMessageStmt->execute();
                    }
                }
                sleep($sleep);
            } catch (\Exception $e) {
                exit($e);
            }
        }
    }
}
