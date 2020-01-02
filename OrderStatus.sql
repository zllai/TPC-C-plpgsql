DROP TYPE IF EXISTS t_status_customer CASCADE;
CREATE TYPE t_status_customer AS (C_ID integer, C_FIRST varchar(32), C_MIDDLE varchar(32), C_LAST varchar(32), C_BALANCE float);
DROP TYPE IF EXISTS t_status_order CASCADE;
CREATE TYPE t_status_order AS (O_ID integer, O_CARRIER_ID integer, O_ENTRY_D timestamp);
DROP TYPE IF EXISTS t_status_order_line CASCADE;
CREATE TYPE t_status_order_line AS (OL_SUPPLY_W_ID smallint, OL_I_ID integer, OL_QUANTITY integer, OL_AMOUNT float, OL_DELIVERY_D timestamp);
DROP TYPE IF EXISTS t_status_res CASCADE;
CREATE TYPE t_status_res AS (customer t_status_customer, _order t_status_order, order_lines t_status_order_line[]);
CREATE OR REPLACE FUNCTION order_status(w_id smallint, 
                             d_id int8,
                             c_id integer,
                             c_last varchar(32))
                             RETURNS t_status_res AS
$$
DECLARE
    getCustomerByCustomerId text := 'SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = $1 AND C_D_ID = $2 AND C_ID = $3';
    getCustomersByLastName text := 'SELECT ARRAY(SELECT (C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE) FROM CUSTOMER WHERE C_W_ID = $1 AND C_D_ID = $2 AND C_LAST = $3 ORDER BY C_FIRST)';
    getLastOrder text := 'SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = $1 AND O_D_ID = $2 AND O_C_ID = $3 ORDER BY O_ID DESC LIMIT 1';
    getOrderLines text := 'SELECT ARRAY(SELECT (OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D) FROM ORDER_LINE WHERE OL_W_ID = $1 AND OL_D_ID = $2 AND OL_O_ID = $3)';
    all_customer t_status_customer[];
    customer t_status_customer;
    _order t_status_order;
    order_lines t_status_order_line[];
BEGIN
    ASSERT w_id IS NOT NULL;
    ASSERT d_id IS NOT NULL;
    IF c_id IS NOT NULL THEN
        EXECUTE getCustomerByCustomerId INTO customer USING w_id, d_id, c_id;
    ELSE
        EXECUTE getCustomersByLastName INTO all_customer USING w_id, d_id, c_last;
        ASSERT all_customer IS NOT NULL;
        customer = all_customer[(array_length(all_customer, 1) - 1) / 2 + 1];
    END IF;
    ASSERT customer IS NOT NULL, 'customer is null';
    c_id = customer.C_ID;
    EXECUTE getLastOrder INTO _order USING w_id, d_id, c_id;
    IF _order IS NOT NULL THEN
        EXECUTE getOrderLines INTO order_lines USING w_id, d_id, _order.O_ID;
    END IF;
    RETURN ROW(customer, _order, order_lines);
END;
$$ LANGUAGE plpgsql;