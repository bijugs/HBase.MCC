HBase.MCC
-----------------------------

### Goal

### Initial Results

![alt tag](https://raw.githubusercontent.com/tmalaska/HBase.MCC/master/AveragePutTimeWithMultiRestartsAndShutDowns.png)

The image above is the initial results of a 20k put test with two clusters.  The blue lines are the average time for the puts to commits to a Cluster.  
There is a design doc in the root folder named MultiHBaseClientDesignDoc.docx.  That document goes over the approach and the configuration options.
