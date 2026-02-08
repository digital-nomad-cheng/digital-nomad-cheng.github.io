import marimo

__generated_with = "0.16.5"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    import polars as pl

    # Left table: Orders
    orders = pl.DataFrame({
        "order_id": [1, 2, 3, 4, 5],
        "customer_id": [101, 102, 103, 104, 105],
        "product": ["A", "B", "A", "C", "B"],
        "amount": [100, 200, 150, 300, 250],
    })

    # Right table: Customers
    customers = pl.DataFrame({
        "customer_id": [101, 102, 103, 106],
        "name": ["Alice", "Bob", "Carol", "David"],
        "city": ["NYC", "LA", "Chicago", "Seattle"],
    })

    # Products reference table
    products = pl.DataFrame({
        "product": ["A", "B", "C", "D"],
        "category": ["Electronics", "Books", "Clothing", "Food"],
        "price": [99, 25, 50, 10],
    })
    return customers, orders, pl, products


@app.cell
def _(orders):
    orders
    return


@app.cell
def _(customers):
    customers
    return


@app.cell
def _(products):
    products
    return


@app.cell
def _(customers, orders):
    # Find orders with valid customers
    inner_result = orders.join(
        customers,
        on="customer_id",
        how="inner"
    )

    print(inner_result)
    return


@app.cell
def _(customers, orders):
    # All orders, enrich with customer info where available
    left_result = orders.join(
        customers,
        on="customer_id",
        how="left"
    )

    print(left_result)
    return


@app.cell
def _(customers, orders):
    # All customers, show their orders (if any)
    right_result = orders.join(
        customers,
        on="customer_id",
        how="right"
    )

    print(right_result)
    return


@app.cell
def _(orders, products):
    # Generate all combinations
    cross_result = orders.join(
        products.select("category").unique(),
        how="cross"
    )

    print(f"Orders: {len(orders)}, Categories: {4}, Result: {len(cross_result)}")
    print(cross_result.head(8))
    return


@app.cell
def _(customers, orders):
    # Find orders from known customers only (filter, don't enrich)
    semi_result = orders.join(
        customers,
        on="customer_id",
        how="semi"
    )

    print(semi_result)
    return


@app.cell
def _(customers, orders):
    # Find orders from unknown/missing customers
    anti_result = orders.join(
        customers,
        on="customer_id",
        how="anti"
    )

    print(anti_result)
    return


@app.cell
def _(pl):
    # Sales data with region
    sales = pl.DataFrame({
        "date": ["2026-01-01", "2026-01-01", "2026-01-02"],
        "region": ["North", "South", "North"],
        "amount": [100, 200, 150]
    })

    # Targets by date and region
    targets = pl.DataFrame({
        "date": ["2026-01-01", "2026-01-01", "2026-01-02"],
        "region": ["North", "South", "North"],
        "target": [120, 180, 140]
    })

    # Join on both date and region
    multi_join = sales.join(
        targets,
        on=["date", "region"],
        how="left"
    ).with_columns(
        (pl.col("amount") - pl.col("target")).alias("variance")
    )

    print(multi_join)
    return sales, targets


@app.cell
def _(sales):
    sales
    return


@app.cell
def _(targets):
    targets
    return


@app.cell
def _(pl):
    # Both tables have 'amount' column
    orders_with_price = pl.DataFrame({
        "product": ["A", "B", "C"],
        "amount": [100, 200, 300],  # order amount
    })

    products_with_cost = pl.DataFrame({
        "product": ["A", "B", "C"],
        "amount": [80, 150, 250],   # product cost
    })

    # Add suffix to distinguish columns
    result = orders_with_price.join(
        products_with_cost,
        on="product",
        how="left",
        suffix="_cost"  # right table columns get this suffix
    )

    print(result)
    # Columns: product, amount, amount_cost
    return


@app.cell
def _(mo):
    mo.md(r"""## Exercise""")
    return


@app.cell
def _(pl):
    from datetime import datetime

    # User registrations
    users_ = pl.DataFrame({
        "user_id": [1, 2, 3, 4, 5],
        "email": ["alice@example.com", "bob@example.com", 
                  "carol@example.com", "david@example.com", "eve@example.com"],
        "signup_date": [datetime(2026, 1, 1), datetime(2026, 1, 5),
                        datetime(2026, 1, 10), datetime(2026, 1, 15),
                        datetime(2026, 1, 20)],
    })

    # Orders placed
    orders_ = pl.DataFrame({
        "order_id": [101, 102, 103, 104, 105, 106],
        "user_id": [1, 2, 1, 3, 99, 2],  # Note: user 99 doesn't exist
        "order_date": [datetime(2026, 1, 5), datetime(2026, 1, 8),
                       datetime(2026, 1, 12), datetime(2026, 1, 15),
                       datetime(2026, 1, 18), datetime(2026, 1, 22)],
        "total": [50.0, 75.5, 120.0, 30.0, 200.0, 45.0],
    })

    # Order items (some orders have multiple items)
    items_ = pl.DataFrame({
        "order_id": [101, 101, 102, 103, 103, 103, 104, 105, 106],
        "product": ["A", "B", "C", "A", "D", "E", "B", "F", "A"],
        "quantity": [2, 1, 3, 1, 2, 1, 1, 5, 2],
        "price": [20.0, 10.0, 25.0, 20.0, 30.0, 50.0, 10.0, 40.0, 20.0],
    })

    # Support tickets
    tickets_ = pl.DataFrame({
        "ticket_id": [1, 2, 3],
        "user_id": [1, 1, 99],  # User 99 doesn't exist
        "issue": ["Refund", "Question", "Complaint"],
    })
    return items_, orders_, users_


@app.cell
def _(orders_, users_):
    # 1. Enric orders
    orders_.join(users_, on="user_id", how="left")
    return


@app.cell
def _(orders_, users_):
    # 2. Find orders placed by non-existent users
    orders_.join(users_, on="user_id", how="anti")
    return


@app.cell
def _(orders_, users_):
    users_.join(orders_, on="user_id", how="semi")
    return


@app.cell
def _(items_, orders_, pl, users_):
    users_.join(orders_, on="user_id", how="full").join(items_, on="order_id", how="full").group_by("user_id").agg(
        pl.col("order_id").n_unique().alias("total_order"),
        pl.col("quantity") * pl.col("price").sum(),
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
