Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=192.168.1.1)(PORT=1521)))(CONNECT_DATA=(SID=th)));User Id=area2;Password=cuuwfxmgdgdldi2;
Barcodes, CarrierId, Timestamp
SELECT * 
FROM BARCODEREADS 
ORDER BY TIMESTAMP DESC
