<?php

namespace Brick\Db\Tests\Bulk;

/**
 * Mocks a PDOStatement for unit testing.
 */
class PDOStatementMock extends \PDOStatement
{
    /**
     * @var PDOMock
     */
    private $pdo;

    /**
     * @var int
     */
    private $number;

    /**
     * @param PDOMock $pdo
     * @param int     $number
     */
    public function __construct(PDOMock $pdo, int $number)
    {
        $this->pdo = $pdo;
        $this->number = $number;
    }

    /**
     * @param array|null $parameters
     *
     * @return bool
     */
    public function execute($parameters = null)
    {
        $this->pdo->log('EXECUTE STATEMENT ' . $this->number . ' (' . $this->dump($parameters) . ')');

        return true;
    }

    /**
     * @param array $parameters
     *
     * @return string
     */
    private function dump(array $parameters) : string
    {
        foreach ($parameters as & $parameter) {
            $parameter = var_export($parameter, true);
        }

        return implode(', ', $parameters);
    }
}
