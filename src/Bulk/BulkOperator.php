<?php

declare(strict_types=1);

namespace Brick\Db\Bulk;

/**
 * Base class for BulkInserter and BulkDeleter.
 */
abstract class BulkOperator
{
    /**
     * The PDO connection.
     */
    private \PDO $pdo;

    /**
     * The name of the target database table.
     *
     * @var string
     */
    protected $table;

    /**
     * The name of the fields to process.
     *
     * @var string[]
     */
    protected $fields;

    /**
     * The number of fields above. This is to avoid redundant count() calls.
     *
     * @var int
     */
    protected $numFields;

    /**
     * The number of records to process per query.
     */
    private int $operationsPerQuery;

    /**
     * The prepared statement to process a full batch of records.
     *
     * @var \PDOStatement
     */
    private $preparedStatement;

    /**
     * A buffer containing the pending values to process in the next batch.
     */
    private array $buffer = [];

    /**
     * The number of operations in the buffer.
     */
    private int $bufferSize = 0;

    /**
     * The total number of operations that have been queued.
     *
     * This includes both flushed and pending operations.
     */
    private int $totalOperations = 0;

    /**
     * The total number of rows affected by flushed operations.
     */
    private int $affectedRows = 0;

    /**
     * Indicates whether the class is in debug mode.
     */
    private bool $debug;

    /**
     * Stores the SQL queries when in debug mode.
     */
    private array $debugQueries = [];

    /**
     * @param \PDO     $pdo                The PDO connection.
     * @param string   $table              The name of the table.
     * @param string[] $fields             The name of the relevant fields.
     * @param int      $operationsPerQuery The number of operations to process in a single query.
     * @param bool     $debug              If true, no writes will be made to the database, 
     *                                     and the proposed SQL can be obtained using getDebugQueries()
     *
     * @throws \InvalidArgumentException
     */
    public function __construct(\PDO $pdo, string $table, array $fields, int $operationsPerQuery = 100, bool $debug = false)
    {
        if ($operationsPerQuery < 1) {
            throw new \InvalidArgumentException('The number of operations per query must be 1 or more.');
        }

        $numFields = count($fields);

        if ($numFields === 0) {
            throw new \InvalidArgumentException('The field list is empty.');
        }

        $this->pdo       = $pdo;
        $this->table     = $table;
        $this->fields    = $fields;
        $this->numFields = $numFields;
        $this->operationsPerQuery = $operationsPerQuery;
        $this->debug = $debug;

        $query = $this->getQuery($operationsPerQuery);
        $this->preparedStatement = $this->pdo->prepare($query);
    }

    /**
     * Queues an operation.
     *
     * @param mixed ...$values An associative array containing values to be processed. 
     *                         Additional keys, not provided in the constructor, will be ignored.
     *
     * @return bool Whether a batch has been synchronized with the database.
     *              This can be used to display progress feedback.
     *
     * @throws \InvalidArgumentException If the number of values does not match the field count.
     */
    public function queue(array $values) : bool
    {
        // Ensure all required fields are present
        foreach ($this->fields as $field) {
            if (!array_key_exists($field, $values)) {
                throw new \InvalidArgumentException(sprintf(
                    'The value for the field "%s" is missing.',
                    $field
                ));
            }
        }

        // Add values in the correct order
        foreach ($this->fields as $field) {
            $this->buffer[] = $values[$field];
        }

        $this->bufferSize++;
        $this->totalOperations++;

        if ($this->bufferSize !== $this->operationsPerQuery) {
            return false;
        }

        $query = $this->getQuery($this->operationsPerQuery);

        if ($this->debug) {
            $this->addQueryToDebug($query, $this->buffer);
        } else {
            $this->preparedStatement->execute($this->buffer);
            $this->affectedRows += $this->preparedStatement->rowCount();
        }

        $this->buffer = [];
        $this->bufferSize = 0;
        
        return true;
    }

    /**
     * Flushes the pending data to the database.
     *
     * This is to be called once after the last queue() has been processed,
     * to force flushing the remaining queued operations to the database table.
     *
     * Do *not* forget to call this method after all the operations have been queued,
     * or it could result in data loss.
     *
     * @return void
     */
    public function flush() : void
    {
        if ($this->bufferSize === 0) {
            return;
        }

        $query = $this->getQuery($this->bufferSize);

        if ($this->debug) {
            $this->addQueryToDebug($query, $this->buffer);
        } else {
            $statement = $this->pdo->prepare($query);
            $statement->execute($this->buffer);
            $this->affectedRows += $statement->rowCount();
        }

        $this->buffer = [];
        $this->bufferSize = 0;
    }

    /**
     * Adds a query to the debug list with the buffer values.
     *
     * @param string $query The query with placeholders.
     * @param array  $buffer The values to replace placeholders.
     *
     * @return void
     */
    private function addQueryToDebug(string $query, array $buffer) : void
    {
        foreach ($buffer as &$value) {
            $value = $this->pdo->quote($value);
        }
        $query = vsprintf(str_replace('?', '%s', $query), $buffer);
        $this->debugQueries[] = $query;
    }
    
    /**
     * Returns the debug queries that have been stored.
     *
     * @return string[] The list of debug queries.
     */
    public function getDebugQueries() : array
    {
        return $this->debugQueries;
    }
    

    /**
     * Resets the bulk operator.
     *
     * This removes any pending operations, and resets the affected row count.
     *
     * @return void
     */
    public function reset() : void
    {
        $this->buffer = [];
        $this->bufferSize = 0;
        $this->affectedRows = 0;
        $this->totalOperations = 0;
        $this->debugQueries = [];
    }

    /**
     * Returns the total number of operations that have been queued.
     *
     * This includes both flushed and pending operations.
     *
     * @return int
     */
    public function getTotalOperations() : int
    {
        return $this->totalOperations;
    }

    /**
     * Returns the number of operations that have been flushed to the database.
     *
     * @return int
     */
    public function getFlushedOperations() : int
    {
        return $this->totalOperations - $this->bufferSize;
    }

    /**
     * Returns the number of pending operations in the buffer.
     *
     * @return int
     */
    public function getPendingOperations() : int
    {
        return $this->bufferSize;
    }

     /**
     * Shows the currently queued SQL query with the data.
     *
     * @return string The currently queued SQL query with the data.
     */
    public function showQueuedQuery() : string
    {
        if (!$this->bufferSize) return "";
        
        $query = $this->getQuery($this->bufferSize);
        $buffer = $this->buffer;
        foreach ($buffer as &$value) {
            // Ensure the value is properly escaped for SQL
            $value = $this->pdo->quote($value);
        }
        $query = vsprintf(str_replace('?', '%s', $query), $buffer);
        return $query;
    }
    
    /**
     * Returns the total number of rows affected by flushed operations.
     *
     * For BulkInserter, this will be equal to the number of operations flushed to the database.
     *
     * @return int
     */
    public function getAffectedRows() : int
    {
        return $this->affectedRows;
    }

    /**
     * @param int $numRecords
     *
     * @return string
     */
    abstract protected function getQuery(int $numRecords) : string;
}
