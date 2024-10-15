<?php

declare(strict_types=1);

namespace Brick\Db\Bulk;

/**
 * Upserts rows into a database table in bulk.
 */
class BulkUpserter extends BulkOperator
{
    /**
     * @inheritdoc
     */
    protected function getQuery(int $numRecords) : string
    {
        $fields       = implode(', ', $this->fields);
        $placeholders = implode(', ', array_fill(0, $this->numFields, '?'));

        $updates = implode(', ', array_map(fn($field) => "$field = VALUES($field)", $this->fields));

        $query  = 'INSERT INTO ' . $this->table . ' (' . $fields . ') VALUES (' . $placeholders . ')';
        $query .= str_repeat(', (' . $placeholders . ')', $numRecords - 1);
        $query .= ' ON DUPLICATE KEY UPDATE ' . $updates;

        return $query;
    }
}
