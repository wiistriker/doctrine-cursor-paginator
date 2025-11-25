<?php

namespace Wiistriker;

use Doctrine\ORM\AbstractQuery;
use Doctrine\ORM\QueryBuilder;
use Symfony\Component\PropertyAccess\PropertyAccess;
use Symfony\Component\PropertyAccess\PropertyAccessorInterface;
use Doctrine\ORM\Query\Expr;

class DoctrineCursorIterator
{
    private PropertyAccessorInterface $propertyAccessor;

    public function __construct(PropertyAccessorInterface $propertyAccessor = null)
    {
        $this->propertyAccessor = $propertyAccessor ?: PropertyAccess::createPropertyAccessor();
    }

    public function iterate(QueryBuilder $qb, int $hydrationMode = AbstractQuery::HYDRATE_OBJECT): iterable
    {
        $last_properties_values = [];
        $end_reached = false;

        $order_by_properties = [];
        $order_by_properties_cnt = 0;
        foreach ($qb->getDQLPart('orderBy') as $orderByPart) {
            $order_by_part = $orderByPart->getParts()[0];
            if (preg_match('/^([a-z0-9_]*)\.([a-z0-9_]*)\s+(ASC|DESC)$/i', $order_by_part, $matches)) {
                $order_field = $matches[1] . '.' . $matches[2];

                $order_by_properties[] = [
                    'field' => $order_field,
                    'property' => $matches[2],
                    'is_asc' => mb_strtolower($matches[3], 'utf-8') === 'asc'
                ];

                $order_by_properties_cnt++;
            }
        }

        if ($order_by_properties_cnt === 0) {
            throw new \RuntimeException('No order properties found');
        }

        $max_results_cnt = $qb->getMaxResults();

        if ($max_results_cnt === null) {
            throw new \RuntimeException('No max results found');
        }

        do {
            $cursorQb = clone($qb);

            if ($last_properties_values) {
                $expr = $qb->expr();

                $nested = null;
                for ($i = $order_by_properties_cnt - 1; $i >= 0; $i--) {
                    $orderByProperty = $order_by_properties[$i];

                    $comparison = new Expr\Comparison(
                        leftExpr: $orderByProperty['field'],
                        operator: $orderByProperty['is_asc'] ? Expr\Comparison::GT : Expr\Comparison::LT,
                        rightExpr: ':' . $orderByProperty['property']
                    );

                    if ($nested === null) {
                        $nested = $comparison;
                    } else {
                        $nested = $expr->orX($comparison, $expr->andX($expr->eq($orderByProperty['field'], ':' . $orderByProperty['property']), $nested));
                    }

                    $cursorQb->setParameter($orderByProperty['property'], $last_properties_values[$orderByProperty['property']]);
                }

                $cursorQb->andWhere($nested);
            }

            $items_cnt = 0;
            foreach ($cursorQb->getQuery()->getResult($hydrationMode) as $item) {
                foreach ($order_by_properties as $orderByProperty) {
                    $property_path = is_array($item) ? '[' . $orderByProperty['property'] . ']' : $orderByProperty['property'];
                    $last_properties_values[$orderByProperty['property']] = $this->propertyAccessor->getValue($item, $property_path);
                }

                yield $item;

                $items_cnt++;
            }

            if ($items_cnt < $max_results_cnt) {
                $end_reached = true;
            }
        } while (!$end_reached);
    }
}
