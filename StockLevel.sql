CREATE FUNCTION stock_level(w_id smallint, 
                            d_id int8,
                            threshold integer)
                            RETURNS integer AS
$$
DECLARE
    getOId text := 'SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = $1 AND D_ID = $2';
    getStockCount text := 'SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK WHERE OL_W_ID = $1 AND OL_D_ID = $2 AND OL_O_ID < $3 AND OL_O_ID >= $4 AND S_W_ID = $5 AND S_I_ID = OL_I_ID AND S_QUANTITY < $6';
    o_id integer;
    result integer;
BEGIN
    EXECUTE getOId INTO o_id USING w_id, d_id;
    ASSERT o_id IS NOT NULL;
    EXECUTE getStockCount INTO result USING w_id, d_id, o_id, o_id-20, w_id, threshold;
    RETURN result;
END;
$$ LANGUAGE plpgsql;
