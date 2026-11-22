#!/bin/bash
# Bulk load of past data into the simulated ERP (dbo.GenerateHistory).
#   ORDERS:     orders to create (default 100000)
#   DAYS:       spread over the last N days (default 180)
#   CUSTOMERS:  customers to add first (default 20000)
#   PRODUCTS:   products to add first (default 300)
# Everything reaches the lake as normal changes: CDC for orders, items,
# payments, shipments and stock; the next scheduled load for customers and products.
set -euo pipefail
ORDERS="${ORDERS:-100000}"; DAYS="${DAYS:-180}"
CUSTOMERS="${CUSTOMERS:-20000}"; PRODUCTS="${PRODUCTS:-300}"
echo "generating $ORDERS orders over $DAYS days, $CUSTOMERS customers, $PRODUCTS products"
start=$(date +%s)
/opt/mssql-tools18/bin/sqlcmd -S sqlserver -U sa -P "$MSSQL_SA_PASSWORD" -C -b \
  -d "$SQLSERVER_DB" -Q "EXEC dbo.GenerateHistory @Orders = $ORDERS, @Days = $DAYS,
      @NewCustomers = $CUSTOMERS, @NewProducts = $PRODUCTS"
echo "done in $(( $(date +%s) - start ))s"
