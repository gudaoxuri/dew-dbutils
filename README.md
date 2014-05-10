## Encapsulate dbutils, Provides dynamic multi-data source , multi-type of database , transaction , paging .

### How to use

Add the following configuration in the properties file  :

    #Support multiple data sources
    ez_multi_ds_support=true
    #Multiple data source configuration items to obtain SQL, Must return the following fields
    ez_multi_ds_query=select code,poolSupport,monitor,driver,url,username,password,initialSize,maxActive,minIdle,maxIdle,maxWait,autoCommit,rmAbandoned,rmAbandonedTimeout,betweenEvictionRuns,minEvictableIdle from multi_ds where enable=1

    #db settings
    ez_db_pool_support=true
    ez_db_pool_monitor=true
    ez_db_pool_initialSize=10
    ez_db_pool_maxActive=50
    ez_db_pool_minIdle=5
    ez_db_pool_maxIdle=20
    ez_db_pool_maxWait=60000
    ez_db_pool_defaultAutoCommit=true
    ez_db_pool_removeAbandoned=true
    ez_db_pool_removeAbandonedTimeoutMillis=180
    ez_db_pool_timeBetweenEvictionRunsMillis=3600000
    ez_db_pool_minEvictableIdleTimeMillis=3600000

    ez_db_jdbc_driver=oracle.jdbc.driver.OracleDriver
    ez_db_jdbc_url=jdbc:oracle:thin:@10.118.128.89:1521:lsgamis
    ez_db_jdbc_username=gajs
    ez_db_jdbc_password=oracle

Reload configuration when data source configuration changes :

    DS.reload()

Transaction support :

    DB db=new DB();
    db.open();
     //do something.
    db.commit();

Paging support :

`Page<Map<String, Object>> page = db.find("<SQL>","<param>"，<page number, starting with 1>, <page size>);`

Java Object package support :

`User user = db.getObject("select * from user where id= ? ", new Object[]{1}, User.class);`

### Building from source
The Project uses a [Maven][]-based build system.

### Check out sources
`git clone https://github.com/gudaoxuri/EZ-DBUtils.git`

### License

Under version 2.0 of the [Apache License][].

[Apache License]: http://www.apache.org/licenses/LICENSE-2.0

[Maven]:http://maven.apache.org/
