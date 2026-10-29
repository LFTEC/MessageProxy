# MIP test results

Executed: 2026-10-29T16:17:15.0357660+08:00

Production endpoint and models executed verbatim in an isolated HTTP host; RabbitMQ startup excluded.

| Test | Result | Details |
| --- | --- | --- |
| Single vendor fulfills entire demand | PASS |  |
| Combined stock cannot fulfill special demand | PASS |  |
| Ordinary demand can split | PASS |  |
| Missing material cannot supply demand | PASS |  |
| Exact stock fulfills special demand | PASS |  |
| Ordinary demand reuses selected supplier | PASS |  |
| Competing special demands respect stock | PASS |  |
| Zero quantity creates no allocation | PASS |  |
| M/N both suppliers have sufficient stock | PASS |  |
| M/N only A can fulfill M and both can fulfill N | PASS |  |
| M/N choose supplier with enough stock for both | PASS |  |
| M/N selected supplier lacks N but combined N is sufficient | PASS |  |
| M/N total N stock insufficient | PASS |  |
| M/N no supplier can fulfill M even with sufficient combined M | PASS |  |
| M/N neither material has sufficient stock | PASS |  |

Passed: 15 / 15

## Single vendor fulfills entire demand

Result: PASS. Total allocated: 100. Vendors: 1.

| Demand | Material | Requested | Single vendor |
| --- | --- | --- | --- |
| R1 | M | 100 | True |

| Vendor | Material | Stock |
| --- | --- | --- |
| A | M | 120 |
| B | M | 60 |

| Demand | Material | Vendor | Allocated |
| --- | --- | --- | --- |
| R1 | M | A | 100 |

## Combined stock cannot fulfill special demand

Result: PASS. Total allocated: 0. Vendors: 0.

| Demand | Material | Requested | Single vendor |
| --- | --- | --- | --- |
| R1 | M | 100 | True |

| Vendor | Material | Stock |
| --- | --- | --- |
| A | M | 60 |
| B | M | 60 |

| Demand | Material | Vendor | Allocated |
| --- | --- | --- | --- |

## Ordinary demand can split

Result: PASS. Total allocated: 100. Vendors: 2.

| Demand | Material | Requested | Single vendor |
| --- | --- | --- | --- |
| R1 | M | 100 | False |

| Vendor | Material | Stock |
| --- | --- | --- |
| A | M | 60 |
| B | M | 60 |

| Demand | Material | Vendor | Allocated |
| --- | --- | --- | --- |
| R1 | M | A | 40 |
| R1 | M | B | 60 |

## Missing material cannot supply demand

Result: PASS. Total allocated: 0. Vendors: 0.

| Demand | Material | Requested | Single vendor |
| --- | --- | --- | --- |
| R1 | M | 100 | True |

| Vendor | Material | Stock |
| --- | --- | --- |
| A | X | 120 |

| Demand | Material | Vendor | Allocated |
| --- | --- | --- | --- |

## Exact stock fulfills special demand

Result: PASS. Total allocated: 100. Vendors: 1.

| Demand | Material | Requested | Single vendor |
| --- | --- | --- | --- |
| R1 | M | 100 | True |

| Vendor | Material | Stock |
| --- | --- | --- |
| A | M | 100 |

| Demand | Material | Vendor | Allocated |
| --- | --- | --- | --- |
| R1 | M | A | 100 |

## Ordinary demand reuses selected supplier

Result: PASS. Total allocated: 120. Vendors: 1.

| Demand | Material | Requested | Single vendor |
| --- | --- | --- | --- |
| R1 | M | 100 | True |
| R2 | M | 20 | False |

| Vendor | Material | Stock |
| --- | --- | --- |
| A | M | 120 |
| B | M | 20 |

| Demand | Material | Vendor | Allocated |
| --- | --- | --- | --- |
| R1 | M | A | 100 |
| R2 | M | A | 20 |

## Competing special demands respect stock

Result: PASS. Total allocated: 100. Vendors: 1.

| Demand | Material | Requested | Single vendor |
| --- | --- | --- | --- |
| R1 | M | 100 | True |
| R2 | M | 100 | True |

| Vendor | Material | Stock |
| --- | --- | --- |
| A | M | 150 |

| Demand | Material | Vendor | Allocated |
| --- | --- | --- | --- |
| R1 | M | A | 100 |

## Zero quantity creates no allocation

Result: PASS. Total allocated: 0. Vendors: 0.

| Demand | Material | Requested | Single vendor |
| --- | --- | --- | --- |
| R1 | M | 0 | True |

| Vendor | Material | Stock |
| --- | --- | --- |
| A | M | 100 |

| Demand | Material | Vendor | Allocated |
| --- | --- | --- | --- |

## M/N both suppliers have sufficient stock

Result: PASS. Total allocated: 180. Vendors: 1.

| Demand | Material | Requested | Single vendor |
| --- | --- | --- | --- |
| R1 | M | 100 | True |
| R2 | N | 80 | False |

| Vendor | Material | Stock |
| --- | --- | --- |
| A | M | 120 |
| A | N | 100 |
| B | M | 120 |
| B | N | 100 |

| Demand | Material | Vendor | Allocated |
| --- | --- | --- | --- |
| R1 | M | A | 100 |
| R2 | N | A | 80 |

## M/N only A can fulfill M and both can fulfill N

Result: PASS. Total allocated: 180. Vendors: 1.

| Demand | Material | Requested | Single vendor |
| --- | --- | --- | --- |
| R1 | M | 100 | True |
| R2 | N | 80 | False |

| Vendor | Material | Stock |
| --- | --- | --- |
| A | M | 100 |
| A | N | 80 |
| B | M | 60 |
| B | N | 80 |

| Demand | Material | Vendor | Allocated |
| --- | --- | --- | --- |
| R1 | M | A | 100 |
| R2 | N | A | 80 |

## M/N choose supplier with enough stock for both

Result: PASS. Total allocated: 180. Vendors: 1.

| Demand | Material | Requested | Single vendor |
| --- | --- | --- | --- |
| R1 | M | 100 | True |
| R2 | N | 80 | False |

| Vendor | Material | Stock |
| --- | --- | --- |
| A | M | 100 |
| A | N | 30 |
| B | M | 100 |
| B | N | 80 |

| Demand | Material | Vendor | Allocated |
| --- | --- | --- | --- |
| R1 | M | B | 100 |
| R2 | N | B | 80 |

## M/N selected supplier lacks N but combined N is sufficient

Result: PASS. Total allocated: 180. Vendors: 2.

| Demand | Material | Requested | Single vendor |
| --- | --- | --- | --- |
| R1 | M | 100 | True |
| R2 | N | 80 | False |

| Vendor | Material | Stock |
| --- | --- | --- |
| A | M | 100 |
| A | N | 30 |
| B | M | 60 |
| B | N | 60 |

| Demand | Material | Vendor | Allocated |
| --- | --- | --- | --- |
| R1 | M | A | 100 |
| R2 | N | A | 20 |
| R2 | N | B | 60 |

## M/N total N stock insufficient

Result: PASS. Total allocated: 150. Vendors: 2.

| Demand | Material | Requested | Single vendor |
| --- | --- | --- | --- |
| R1 | M | 100 | True |
| R2 | N | 80 | False |

| Vendor | Material | Stock |
| --- | --- | --- |
| A | M | 100 |
| A | N | 30 |
| B | M | 60 |
| B | N | 20 |

| Demand | Material | Vendor | Allocated |
| --- | --- | --- | --- |
| R1 | M | A | 100 |
| R2 | N | A | 30 |
| R2 | N | B | 20 |

## M/N no supplier can fulfill M even with sufficient combined M

Result: PASS. Total allocated: 80. Vendors: 1.

| Demand | Material | Requested | Single vendor |
| --- | --- | --- | --- |
| R1 | M | 100 | True |
| R2 | N | 80 | False |

| Vendor | Material | Stock |
| --- | --- | --- |
| A | M | 60 |
| A | N | 80 |
| B | M | 60 |
| B | N | 80 |

| Demand | Material | Vendor | Allocated |
| --- | --- | --- | --- |
| R2 | N | B | 80 |

## M/N neither material has sufficient stock

Result: PASS. Total allocated: 50. Vendors: 2.

| Demand | Material | Requested | Single vendor |
| --- | --- | --- | --- |
| R1 | M | 100 | True |
| R2 | N | 80 | False |

| Vendor | Material | Stock |
| --- | --- | --- |
| A | M | 60 |
| A | N | 30 |
| B | M | 60 |
| B | N | 20 |

| Demand | Material | Vendor | Allocated |
| --- | --- | --- | --- |
| R2 | N | A | 30 |
| R2 | N | B | 20 |
