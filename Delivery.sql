DROP TYPE IF EXISTS t_delivery_res CASCADE;
CREATE TYPE t_delivery_res AS (d_id integer, no_o_id integer);


CREATE OR REPLACE FUNCTION delivery(w_id integer, 
                         o_carrier_id integer, 
                         ol_delivery_d timestamp) 
                         RETURNS SETOF t_delivery_res AS
$$
DECLARE
    getNewOrder     CONSTANT text := 'SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = $1 AND NO_W_ID = $2 AND NO_O_ID > -1 LIMIT 1';
    deleteNewOrder  CONSTANT text := 'DELETE FROM NEW_ORDER WHERE NO_D_ID = $1 AND NO_W_ID = $2 AND NO_O_ID = $3';
    getCId          CONSTANT text := 'SELECT O_C_ID FROM ORDERS WHERE O_ID = $1 AND O_D_ID = $2 AND O_W_ID = $3';
    updateOrders    CONSTANT text := 'UPDATE ORDERS SET O_CARRIER_ID = $1 WHERE O_ID = $2 AND O_D_ID = $3 AND O_W_ID = $4';
    updateOrderLine CONSTANT text := 'UPDATE ORDER_LINE SET OL_DELIVERY_D = $1 WHERE OL_O_ID = $2 AND OL_D_ID = $3 AND OL_W_ID = $4';
    sumOLAmount     CONSTANT text := 'SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_O_ID = $1 AND OL_D_ID = $2 AND OL_W_ID = $3';
    updateCustomer  CONSTANT text := 'UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + $1 WHERE C_ID = $2 AND C_D_ID = $3 AND C_W_ID = $4'; 
    districts_per_warehouse CONSTANT int8 := 10;
    no_o_id     integer;
    c_id        integer;
    ol_total    integer;
    res         t_delivery_res;
BEGIN
    FOR d_id IN 1..districts_per_warehouse LOOP
        EXECUTE getNewOrder INTO no_o_id USING d_id, w_id;
        IF no_o_id = NULL THEN
            CONTINUE;
        END IF;
        EXECUTE getCId INTO c_id USING no_o_id, d_id, w_id;
        EXECUTE sumOLAmount INTO ol_total USING no_o_id, d_id, w_id;

        EXECUTE deleteNewOrder USING d_id, w_id, no_o_id;
        EXECUTE updateOrders USING o_carrier_id, no_o_id, d_id, w_id;
        EXECUTE updateOrderLine USING ol_delivery_d, no_o_id, d_id, w_id;
        EXECUTE updateCustomer USING ol_total, c_id, d_id, w_id;
        res.d_id = d_id;
        res.no_o_id = no_o_id;
        RETURN NEXT res;
    END LOOP;
    RETURN;
END;
$$ LANGUAGE plpgsql;