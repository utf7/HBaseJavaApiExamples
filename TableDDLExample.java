package com.utf7.hbase.hbase.api;


public class TableDDLExamples {
   Configuration conf = HBaseConfiguration.create();
    String tablename = "test";
    String columnFamily = "cf";
    Admin admin = null;
    Table table = null;

    @Before
    public void init() {

	try {
	    org.apache.hadoop.hbase.client.Connection conn = ConnectionFactory
		    .createConnection(conf);
	    admin = conn.getAdmin();
	    table = conn.getTable(TableName.valueOf(tablename));
	} catch (IOException e) {
	    e.printStackTrace();
	}

    }

    @Test
    public void createTable() throws Exception {
	if (admin.tableExists(TableName.valueOf(tablename))) {
	    System.out.println("table Exists!");
	    System.exit(0);
	} else {
	    HTableDescriptor tableDesc = new HTableDescriptor(
		    TableName.valueOf(tablename));
	    tableDesc.addFamily(new HColumnDescriptor(columnFamily));
	    admin.createTable(tableDesc);
	    System.out.println("create table success!");
	}
    }

    @Test
    public void deleteTable() throws IOException {

	TableName tableName = TableName.valueOf(tablename);
	if (admin.tableExists(tableName)) {
	    if (admin.isTableEnabled(tableName)) {
		admin.disableTable(tableName);
	    }
	    admin.deleteTable(tableName);

	}
    }
    
    @Test
    public void disableTable() throws IOException {

	TableName tableName = TableName.valueOf(tablename);
	if (admin.tableExists(tableName)) {
	    if (admin.isTableEnabled(tableName)) {
		admin.disableTable(tableName);
	    }
	}
    }
    
    }
