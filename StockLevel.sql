CREATE FUNCTION stock_level(w_id smallint, 
                            d_id tinyint,
                            threshold integer)
                            RETURNS RECORD AS
$$
DECLARE
    getOId text := 'SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = $1 AND D_ID = $2';
    getStockCount text := 'SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK WHERE OL_W_ID = $1 AND OL_D_ID = $2 AND OL_O_ID < $3 AND OL_O_ID >= $4 AND S_W_ID = $5 AND S_I_ID = OL_I_ID AND S_QUANTITY < $6';
    result RECORD;
BEGIN
    EXECUTE getOId INTO result USING w_id, d_id;
    ASSERT result != NULL;
    EXECUTE getStockCount INTO result USING w_id, d_id, result.D_NEXT_O_ID, result.D_NEXT_O_ID-20, w_id, threshold;
    RETURN result;
END;
$$ LANGUAGE plpgsql;
