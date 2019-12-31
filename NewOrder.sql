CREATE FUNCTION new_order(w_id smallint, 
                          d_id tinyint,
                          c_id integer,
                          o_entry_d timestamp,
                          i_ids integer[],
                          i_w_ids integer[],
                          i_qtys integer[]) 
                          RETURNS RECORD AS
$$
DECLARE
    getWarehouseTaxRate text := 'SELECT W_TAX FROM WAREHOUSE WHERE W_ID = $1';
    getDistrict text := 'SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = $1 AND D_W_ID = $2';
    incrementNextOrderId text := 'UPDATE DISTRICT SET D_NEXT_O_ID = $1 WHERE D_ID = $2 AND D_W_ID = $3';
    getCustomer text := 'SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = $1 AND C_D_ID = $2 AND C_ID = $3';
    createOrder text := 'INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)';
    createNewOrder text := 'INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES ($1, $2, $3)';
    getItemInfo text := 'SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = $1';
    getStockInfo text := 'SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_%s AS S_DIST FROM STOCK WHERE S_I_ID = $1 AND S_W_ID = $2';
    updateStock text := 'UPDATE STOCK SET S_QUANTITY = $1, S_YTD = $2, S_ORDER_CNT = $3, S_REMOTE_CNT = $4 WHERE S_I_ID = $5 AND S_W_ID = $6';
    createOrderLine text := 'INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)';

    all_local boolean := TRUE;
    items RECORD[];
    item RECORD;
    w_tax float;
    d_tax float;
    d_next_o_id integer;
    customer_info RECORD;
    c_discount float;
    ol_cnt integer;
    o_carrier_id integer;
    stock_info RECORD;
    brand_generic char;
    ol_amount float;
    total float := 0;
    item_data RECORD;
    misc RECORD;
BEGIN
    ol_cnt = array_length(i_ids);
    ASSERT ol_cnt ! 0;
    ASSERT ol_cnt = array_length(i_w_ids);
    ASSERT ol_cnt = array_length(i_qtys);

    FOR i IN 1..ol_cnt LOOP
        all_local = all_local AND id_w_ids[i] == w_id;
        EXECUTE getItemInfo INTO item USING i_ids[i];
        ASSERT item != NULL;
        items = array_append(items, item);
    END LOOP;
    ASSERT ol_cnt = array_length(items);
    EXECUTE getWarehouseTaxRate INTO w_tax USING w_id;
    EXECUTE getDistrict INTO d_tax, d_next_o_id USING d_id, w_id;
    EXECUTE getCustomer INTO customer_info USING w_id, d_id, c_id;
    c_discount = customer_info.C_DISCOUNT;

    o_carrier_id = 0;
    EXECUTE incrementNextOrderId USING d_next_o_id+1, d_id, w_id;
    EXECUTE createOrder USING d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, ol_cnt, all_local;
    EXECUTE createNewOrder USING d_next_o_id, d_id, w_id;

    FOR i IN 1..ol_cnt LOOP
        EXECUTE FORMAT(getStockInfo, to_char(d_id, '09')) INTO stock_info USING i_ids[i], i_w_ids[i];
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

        IF position('ORIGINAL', itmes[i].I_DATA) != 0 AND position('ORIGINAL', stock_info.S_DATA) != 0 THEN
            brand_generic = 'B';
        ELSE
            brand_generic = 'G';
        END IF;

        ol_amount = i_qtys[i] * itmes[i].I_PRICE;
        total = total + ol_amount;
        EXECUTE createOrderLine USING d_next_o_id, d_id, w_id, i, i_ids[i], i_w_ids[i], o_entry_d, i_qtys[i], ol_amount, stock_info.S_DIST;
        item_data = array_append(item_data, ROW(items[i].I_NAME, stock_info.S_QUANTITY, brand_generic, items[i].I_PRICE, ol_amount));
    END LOOP;
    total = total * (1 - c_discount) * (1 + w_tax + d_tax);
    misc = ROW(w_tax, d_tax, d_next_o_id, total);
    RETURN [customer_info, misc, item_data];
END;
$$ LANGUAGE plpgsql;
