DROP PROCEDURE IF EXISTS prod.pro4();

COMMIT;


CREATE OR REPLACE PROCEDURE prod.pro4()
    LANGUAGE plpgsql AS
$$
BEGIN
    SELECT 1;
END;
$$;

COMMIT;