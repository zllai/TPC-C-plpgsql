CREATE FUNCTION order_status(w_id smallint, 
                             d_id tinyint,
                             c_id integer,
                             c_last varchar(32))
                             RETURNS RECORD AS
$$
DECLARE
    getCustomerByCustomerId text := 'SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = $1 AND C_D_ID = $2 AND C_ID = $3';
    getCustomersByLastName text := 'SELECT ARRAY(SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = $1 AND C_D_ID = $2 AND C_LAST = $3 ORDER BY C_FIRST)';
    getLastOrder text := 'SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = $1 AND O_D_ID = $2 AND O_C_ID = $3 ORDER BY O_ID DESC LIMIT 1';
    getOrderLines text := 'SELECT ARRAY(SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = $1 AND OL_D_ID = $2 AND OL_O_ID = $3)';
    all_customer RECORD[];
    customer RECORD;
    order RECORD;
    order_lines RECORD[];
BEGIN
    ASSERT w_id != NULL;
    ASSERT d_id != NULL;
    IF c_id != NULL THEN
        EXECUTE getCustomerByCustomerId INTO customer USING w_id, d_id, c_id;
    ELSE
        EXECUTE getCustomersByLastName INTO all_customer USING w_id, d_id, c_last;
        ASSERT all_customer != NULL;
        customer = all_customer[(array_length(all_customer) - 1) / 2];
    END IF;
    ASSERT customer != NULL;
    c_id = customer.C_ID;
    EXECUTE getLastOrder INTO order USING w_id, d_id, c_id;
    IF order != NULL THEN
        EXECUTE getOrderLines INTO order_lines USING w_id, d_id, order;
    END IF;
    RETURN ROW(customer, order, order_lines);
END;
$$ LANGUAGE plpgsql;