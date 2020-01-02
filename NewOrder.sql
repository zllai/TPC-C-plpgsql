DROP TYPE IF EXISTS t_order_item CASCADE;
CREATE TYPE t_order_item AS (I_PRICE float, I_NAME varchar(32), I_DATA varchar(64));
DROP TYPE IF EXISTS t_order_customer CASCADE;
CREATE TYPE t_order_customer AS (C_DISCOUNT float, C_LAST varchar(32), C_CREDIT varchar(2));
DROP TYPE IF EXISTS t_order_stock CASCADE;
CREATE TYPE t_order_stock AS (S_QUANTITY integer, S_DATA varchar(64), S_YTD integer, S_ORDER_CNT integer, S_REMOTE_CNT integer, S_DIST varchar(32));
DROP TYPE IF EXISTS t_order_item_data CASCADE;
CREATE TYPE t_order_item_data AS (I_NAME varchar(32), S_QUANTITY integer, brand_generic char, I_PRICE float, OL_AMOUNT float);
DROP TYPE IF EXISTS t_order_misc CASCADE;
CREATE TYPE t_order_misc AS (w_tax float, d_tax float, d_next_o_id integer, total float);
DROP TYPE IF EXISTS t_order_res CASCADE;
CREATE TYPE t_order_res AS (customer_info t_order_customer, misc t_order_misc, item_data t_order_item_data[]);

CREATE OR REPLACE FUNCTION new_order(w_id smallint, 
                           d_id int8,
                           c_id integer,
                           o_entry_d timestamp,
                           i_ids integer[],
                           i_w_ids integer[],
                           i_qtys integer[]) 
                           RETURNS t_order_res AS
$$
DECLARE
    getWarehouseTaxRate text := 'SELECT W_TAX FROM WAREHOUSE WHERE W_ID = $1';
    getDistrict text := 'SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = $1 AND D_W_ID = $2';
    incrementNextOrderId text := 'UPDATE DISTRICT SET D_NEXT_O_ID = $1 WHERE D_ID = $2 AND D_W_ID = $3';
    getCustomer text := 'SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = $1 AND C_D_ID = $2 AND C_ID = $3';
    createOrder text := 'INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)';
    createNewOrder text := 'INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES ($1, $2, $3)';
    getItemInfo text := 'SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = $1';
    getStockInfo text := 'SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_%s FROM STOCK WHERE S_I_ID = $1 AND S_W_ID = $2';
    updateStock text := 'UPDATE STOCK SET S_QUANTITY = $1, S_YTD = $2, S_ORDER_CNT = $3, S_REMOTE_CNT = $4 WHERE S_I_ID = $5 AND S_W_ID = $6';
    createOrderLine text := 'INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)';

    all_local boolean := TRUE;
    ol_cnt integer;
    items t_order_item[];
    item t_order_item;
    customer_info t_order_customer;
    c_discount float;
    o_carrier_id integer;
    stock_info t_order_stock;
    w_tax float;
    d_tax float;
    d_next_o_id integer;
    brand_generic char;
    ol_amount float;
    total float := 0;
    item_data_s t_order_item_data;
    item_data t_order_item_data[];
    misc t_order_misc;
BEGIN
    ol_cnt = array_length(i_ids, 1);
    ASSERT ol_cnt != 0;
    ASSERT ol_cnt = array_length(i_w_ids, 1);
    ASSERT ol_cnt = array_length(i_qtys, 1);

    FOR i IN 1..ol_cnt LOOP
        all_local = all_local AND i_w_ids[i] = w_id;
        EXECUTE getItemInfo INTO item USING i_ids[i];
        ASSERT item IS NOT NULL;
        items = array_append(items, item);
    END LOOP;
    ASSERT ol_cnt = array_length(items, 1);
    EXECUTE getWarehouseTaxRate INTO w_tax USING w_id;
    EXECUTE getDistrict INTO d_tax, d_next_o_id USING d_id, w_id;
    EXECUTE getCustomer INTO customer_info USING w_id, d_id, c_id;
    c_discount = customer_info.C_DISCOUNT;

    o_carrier_id = 0;
    EXECUTE incrementNextOrderId USING d_next_o_id+1, d_id, w_id;
    EXECUTE createOrder USING d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, ol_cnt, cast(all_local as integer);
    EXECUTE createNewOrder USING d_next_o_id, d_id, w_id;

    FOR i IN 1..ol_cnt LOOP
        EXECUTE FORMAT(getStockInfo, to_char(d_id, 'FM09')) INTO stock_info USING i_ids[i], i_w_ids[i];
        IF stock_info = NULL THEN
            RAISE WARNING 'No STOCK record for (ol_i_id=%, ol_supply_w_id=%)', id_ids[i], i_w_ids[i];
            CONTINUE;
        END IF;
        stock_info.S_YTD = stock_info.S_YTD + i_qtys[i];
        IF stock_info.S_QUANTITY >= i_qtys[i] + 10 THEN
            stock_info.S_QUANTITY = stock_info.S_QUANTITY - i_qtys[i];
        ELSE
            stock_info.S_QUANTITY = stock_info.S_QUANTITY + 91 - i_qtys[i];
        END IF;
        stock_info.S_ORDER_CNT = stock_info.S_ORDER_CNT + 1;

        IF i_w_ids[i] != w_id THEN
            stock_info.S_REMOTE_CNT = stock_info.S_REMOTE_CNT + 1;
        END IF;

        EXECUTE updateStock USING stock_info.S_QUANTITY, stock_info.S_YTD, stock_info.S_ORDER_CNT, stock_info.S_REMOTE_CNT, i_ids[i], i_w_ids[i];

        IF position('ORIGINAL' IN items[i].I_DATA) != 0 AND position('ORIGINAL' IN stock_info.S_DATA) != 0 THEN
            brand_generic = 'B';
        ELSE
            brand_generic = 'G';
        END IF;

        ol_amount = i_qtys[i] * items[i].I_PRICE;
        total = total + ol_amount;
        EXECUTE createOrderLine USING d_next_o_id, d_id, w_id, i, i_ids[i], i_w_ids[i], o_entry_d, i_qtys[i], ol_amount, stock_info.S_DIST;
        item_data_s = ROW(items[i].I_NAME, stock_info.S_QUANTITY, brand_generic, items[i].I_PRICE, ol_amount);
        item_data = array_append(item_data, item_data_s);
    END LOOP;
    total = total * (1 - c_discount) * (1 + w_tax + d_tax);
    misc = ROW(w_tax, d_tax, d_next_o_id, total);
    RETURN ROW(customer_info, misc, item_data);
END;
$$ LANGUAGE plpgsql;
