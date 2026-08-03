merge into ${ecom_dim_schema}.order_sku dest
using (
    select chain_key, web_ord_num, dks_sku, src.ship_class, src.ship_mode
    from ${ecom_dim_schema}.order_sku
    join(
        select ship_class, ship_mode, sku, order_number
        from  ${co_stage_schema}.ORDER_MESSAGE_PLACED
        where line_item_type = 'Product'
        union
        select ship_class, ship_mode, sku, order_number
        from  ${co_stage_schema}.ORDER_MESSAGE_CANCEL
        where line_item_type = 'Product'
    ) as src
    on web_ord_num = order_number and dks_sku = sku
    where cust_fulfillment_mode_key = -1
    and ship_class = 'PARCEL'
    and ship_mode = 'SameDay'
) as src
     on dest.web_ord_num = src.web_ord_num
     and dest.chain_key = src.chain_key
     and dest.dks_sku = src.dks_sku
     when matched then
     update set dest.cust_fulfillment_mode_key = 298;
