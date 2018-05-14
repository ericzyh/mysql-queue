<?php

namespace MysqlQueue;

//入列类
class Publisher
{
    public $pdo = null;

    public static $defaultMessageOption = [
        "delay" => 0,
        "priority" => 1,
        "routing_key" => '',
    ];
    /**
     * 连接mysql数据库
     */
    public function connect($host, $port, $user, $password, $database)
    {
        try {
            $this->pdo = new \Pdo("mysql:host=$host;port=$port;dbname=$database", $user, $password);
        } catch (\Exception $e) {
            die("error in connecting to database");
        }
    }

    /**
     * 消息入列
     */
    public function publish($queueName, $message, $messageOption = [])
    {
        $queueName || die("queue name cannot be null");
        $message || die("message cannot be null");
        is_array($message) && $message = json_encode($message, true);
        $messageOption = self::$defaultMessageOption + $messageOption;
        $messageRecord = [
            ":queue_name" => $queueName,
            ":message" => $message,
            ":add_time" => time(),
            ":delay" => $messageOption['delay'],
            ":priority" => $messageOption['priority'],
            ":routing_key" => $messageOption['routing_key'],
        ];
        $insertStmt = $this->pdo->prepare(
            "insert into message_queue(queue_name, message, add_time, delay, priority, routing_key)
            values(:queue_name, :message, :add_time, :delay, :priority, :routing_key)"
        );
        $insertStmt->execute($messageRecord);
        $errInfo = $insertStmt->errorInfo();
        if ($errInfo[0] != 00000) {
            return $errInfo;
        }
        return $this->pdo->lastInsertId();
    }
}
