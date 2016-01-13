This is a fork of 

https://github.com/ngageoint/elasticgeo

So all licenses apply and original source is there...


This for was done to try and port to Elasticsearch 2.x apis.

This only attempts to port to get all the tests to pass, it is not known if works in geoserver...


Some of the changes:

1) Removed the test quadtree levels setting, it was causing the index to be over 400 MB for just a few simple geo json records used in tests.

2) Updated the connect() method in ElasticTestSupport to use system line endings so people on windows with git replacing LF with CRLF can build the project without errors.

3) I think all tests no longer use elasticsearch cluster name

4) The biggest refactor was from Elasticsearch no longer having a FilterBuilder API.  I left original code commented because I probably made a few mistakes.  I have not updated any documentation, other
than remove javadoc links that break in jdk 1.8 builds.

5) changed the artifact id in poms, appending -2x.  

6) 