
"""
Transform Data
"""

import pandas as pd


# ==================================================
# Transform Data
# ==================================================

def transform_data(all_dataset: dict):

    # ========================================
    # 1. Convert Datatime Columns
    # ========================================

    date_columns = {
        'orders': [
            'order_purchase_timestamp', 'order_approved_at'
            , 'order_delivered_carrier_date', 'order_delivered_customer_date'
            , 'order_estimated_delivery_date'
        ]
        , 'order_items': ['shipping_limit_date']
        , 'reviews': ['review_creation_date', 'review_answer_timestamp']
    }

    for table, columns in date_columns.items():
        for col in columns:
            if col in all_dataset[table].columns:
                all_dataset[table][col] = pd.to_datetime(all_dataset[table][col], errors='coerce')

    # ========================================
    # 2. Create derived metrics for orders
    # ========================================

    orders = all_dataset['orders'].copy()

    # Delivery time metrics
    orders['delivery_days'] = (
        orders['order_delivered_customer_date'] - orders['order_purchase_timestamp']
    ).dt.days
    
    orders['estimated_delivery_days'] = (
        orders['order_estimated_delivery_date'] - orders['order_purchase_timestamp']
    ).dt.days
    
    orders['delivery_delay_days'] = (
        orders['order_delivered_customer_date'] - orders['order_estimated_delivery_date']
    ).dt.days
    
    orders['is_late'] = orders['delivery_delay_days'] > 0

    # Time dimensions
    orders['order_year'] = orders['order_purchase_timestamp'].dt.year
    orders['order_month'] = orders['order_purchase_timestamp'].dt.month
    orders['order_quarter'] = orders['order_purchase_timestamp'].dt.quarter
    orders['order_day_of_week'] = orders['order_purchase_timestamp'].dt.dayofweek
    orders['order_hour'] = orders['order_purchase_timestamp'].dt.hour
    orders['order_date'] = orders['order_purchase_timestamp'].dt.date
    
    all_dataset['orders'] = orders

    # ========================================
    # 3. Aggregate order items to order level
    # ========================================

    order_items = all_dataset['order_items'].copy()

    order_summary = order_items.groupby('order_id').agg({
        'price': 'sum'
        , 'freight_value': 'sum'
        , 'order_item_id': 'count'
        , 'product_id': 'nunique'
        , 'seller_id': 'nunique'
    }).reset_index()
    
    order_summary.columns = [
        'order_id', 'total_price', 'total_freight'
        , 'total_items', 'unique_products', 'unique_sellers'
    ]
    order_summary['total_order_value'] = order_summary['total_price'] + order_summary['total_freight']
    
    all_dataset['order_summary'] = order_summary

    # ========================================
    # 4. Create fact table: orders with all metrics
    # ========================================

    fact_orders = orders.merge(order_summary, on='order_id', how='left')

    # Add customer state
    fact_orders = fact_orders.merge(
        all_dataset['customers'][['customer_id', 'customer_state', 'customer_city']]
        , on='customer_id', how='left'
    )

    # Add review score
    reviews_agg = all_dataset['reviews'].groupby('order_id')['review_score'].mean().reset_index()
    fact_orders = fact_orders.merge(reviews_agg, on='order_id', how='left')
    
    all_dataset['fact_orders'] = fact_orders

    # ========================================
    # 5. Create product performance table
    # ========================================

    product_perf = order_items.merge(
        all_dataset['products'][['product_id', 'product_category_name', 'base_price']]
        , on='product_id', how='left'
    )

    product_perf = product_perf.merge(
        orders[['order_id', 'order_purchase_timestamp', 'order_status']]
        , on='order_id', how='left'
    )

    product_summary = product_perf.groupby(['product_id', 'product_category_name']).agg({
        'order_id': 'count'
        , 'price': 'sum'
        , 'order_status': lambda x: (x == 'delivered').sum()
    }).reset_index()

    product_summary.columns = [
        'product_id', 'category', 'total_orders'
        , 'total_revenue', 'delivered_orders'
    ]
    
    all_dataset['product_summary'] = product_summary

    # ========================================
    # 6. Create seller performance table
    # ========================================

    seller_items = order_items.merge(
        orders[['order_id', 'order_status']]
        , on='order_id'
    )
    seller_items = seller_items.merge(
        all_dataset['reviews'][['order_id', 'review_score']]
        , on='order_id', how='left'
    )

    seller_summary = seller_items.groupby('seller_id').agg({
        'order_id': 'nunique'
        , 'price': 'sum'
        , 'review_score': 'mean'
        , 'order_status': lambda x: (x == 'canceled').sum()
    }).reset_index()

    seller_summary.columns = [
        'seller_id', 'total_orders', 'total_revenue'
        , 'avg_rating', 'canceled_orders'
    ]
    
    seller_summary = seller_summary.merge(
        all_dataset['sellers'][['seller_id', 'seller_state', 'seller_city']]
        , on='seller_id', how='left'
    )
    
    all_dataset['seller_summary'] = seller_summary

    # ========================================
    # 7. Create daily metrics table
    # ========================================

    daily_metrics = fact_orders[fact_orders['order_status'] != 'canceled'].groupby('order_date').agg({
        'order_id': 'count'
        , 'total_order_value': 'sum'
        , 'total_items': 'sum'
        , 'customer_id': 'nunique'
        , 'review_score': 'mean'
        , 'is_late': 'mean'
    }).reset_index()

    daily_metrics.columns = [
        'date', 'orders', 'revenue', 'items_sold'
        , 'unique_customers', 'avg_rating', 'late_delivery_rate'
    ]
    daily_metrics['date'] = pd.to_datetime(daily_metrics['date'])

    # Calculate 7-day rolling averages
    daily_metrics['revenue_7d_avg'] = daily_metrics['revenue'].rolling(7, min_periods=1).mean()
    daily_metrics['orders_7d_avg'] = daily_metrics['orders'].rolling(7, min_periods=1).mean()

    all_dataset['daily_metrics'] = daily_metrics

    # ========================================
    # 8. Create category performance table
    # ========================================

    category_perf = product_perf[product_perf['order_status'] != 'canceled'].groupby('product_category_name').agg({
        'order_id': 'count'
        , 'price': 'sum'
        , 'product_id': 'nunique'
    }).reset_index()

    category_perf.columns = ['category', 'total_orders', 'total_revenue', 'unique_products']
    category_perf['avg_order_value'] = category_perf['total_revenue'] / category_perf['total_orders']
    category_perf = category_perf.sort_values('total_revenue', ascending=False)

    all_dataset['category_summary'] = category_perf

    # ========================================
    # 9. Create state-level metrics
    # ========================================

    state_metrics = fact_orders[fact_orders['order_status'] != 'canceled'].groupby('customer_state').agg({
        'order_id': 'count'
        , 'total_order_value': 'sum'
        , 'customer_id': 'nunique'
        , 'review_score': 'mean'
        , 'delivery_days': 'mean'
    }).reset_index()

    state_metrics.columns = [
        'state', 'total_orders', 'total_revenue'
        , 'unique_customers', 'avg_rating', 'avg_delivery_days'
    ]
    
    all_dataset['state_metrics'] = state_metrics

    # ========================================
    # Return
    # ========================================

    return all_dataset

