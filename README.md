# Sclera - Oracle Connector

Enables Sclera to work with your data stored in [Oracle](http://www.oracle.com).

You just need to link your Oracle database with Sclera, then import the metadata of select tables within the database. All this gets done in a couple of commands -- and enables you to include these tables within your Sclera queries. Details on how to link your Oracle source to with Sclera can be found in the [Sclera Database System Connection Reference](https://www.scleradb.com/doc/ref/dbms#connecting-to-oracle) document.

This component uses the [Oracle JDBC Driver](https://www.oracle.com/database/technologies/appdev/jdbc.html), which is *not* downloaded as a part of the installation of this component. You need to download the driver manually ([download link](https://www.oracle.com/database/technologies/appdev/jdbc-ucp-183-downloads.html)) before using this component and place it in a location included in the CLASSPATH environment variable. You will need to accept Oracle's licence and usage conditions before downloading the JDBC driver.
