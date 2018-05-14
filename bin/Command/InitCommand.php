<?php

namespace MysqlQueueBin\Command;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Question\Question;


/**
 * 创建消息队列表，持久化数据库配置
 */
class InitCommand extends Command
{
    public $sql = <<<EOL
        CREATE TABLE `message_queue` (
          `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'id',
          `queue_name` char(255) NOT NULL COMMENT '队列名称',
          `message` varchar(1024) NOT NULL COMMENT 'json格式消息实体',
          `add_time` int(11) NOT NULL COMMENT '消息添加时间',
          `delay` int(11) NOT NULL DEFAULT '0' COMMENT '消息延时',
          `priority` tinyint(4) NOT NULL DEFAULT '1' COMMENT '消息优先级, 最小0，最大127，优先级越高越优先处理',
          `routing_key` char(255) NOT NULL DEFAULT '' COMMENT '消息路由键',
          `status` tinyint(4) NOT NULL DEFAULT '0' COMMENT '消息状态，0为未处理，2为已处理，4为处理错误',
          PRIMARY KEY (`id`),
          KEY `idx_name_status_addtime` (`queue_name`,`status`,`add_time`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8
EOL;

    protected function configure()
    {
        $this->setName("mysql:init");
        $this->setHelp("init mysql configuration");
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        //acquire database information
        $output->writeln("<info>starting to init mysql information</info>");
        $helper = $this->getHelper("question");
        $host = new Question("mysql host: ");
        $port = new Question("mysql port: ");
        $user = new Question("mysql user: ");
        $password = new Question("mysql password: ");
        $password->setHidden(true);
        $password->setHiddenFallback(false);
        $database = new Question("mysql database: ");
        $host = $helper->ask($input, $output, $host);
        $port = $helper->ask($input, $output, $port);
        $user = $helper->ask($input, $output, $user);
        $password = $helper->ask($input, $output, $password);
        $database = $helper->ask($input, $output, $database);
        $output->writeln("<info>connecting to mysql</info>");
        //create message_queue table
        try {
            $pdo = new \PDO("mysql:host=$host;port=$port;dbname=$database", $user, $password);
        } catch (\Exception $e) {
            $msg = $e->getMessage();
            $output->writeln("<error>$msg</error>");
            return;
        }
        $pdo->exec("set names UTF8");
        $existingTableStmt = $pdo->prepare("show tables like 'message_queue'");
        $existingTableStmt->execute();
        $existingTable = $existingTableStmt->fetch();
        if ($existingTable) {
            $output->writeln("<error>table already exists</error>");
        } else {
            try {
                $createTableStmt = $pdo->prepare($this->sql);
                $createTableStmt->execute();
            } catch(\Exception $e) {
                $output->writeln("<error>error in creating message_queue table</info>");
                return;
            }
            $output->writeln("<info>message_queue table created successfully</info>");
        }
    }
}
