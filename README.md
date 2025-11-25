# Doctrine Cursor Iterator for large datasets

Iterate through large database results with easy

## Usage

Create query builder as usual with order by and max results

```php
$testEntityRepository = $this->entityManager->getRepository(TestEntity::class);
$qb = $testEntityRepository->createQueryBuilder('t')
    ->orderBy('t.id', 'ASC')
    ->setMaxResults(100)
;

$cursorIterator = new DoctrineCursorIterator();

foreach ($cursorIterator->iterate($qb) as $testEntity) {
    //...
}
```

You can also specify more order by fields

```php
$testEntityRepository = $this->entityManager->getRepository(TestEntity::class);
$qb = $testEntityRepository->createQueryBuilder('t')
    ->select('t.id', 't.createdAt')
    ->orderBy('t.createdAt', 'DESC')
    ->addOrderBy('t.id', 'DESC')
    ->setMaxResults(100)
;

$cursorIterator = new DoctrineCursorIterator();

foreach ($cursorIterator->iterate($qb) as $testEntity) {
    //...
}
```
