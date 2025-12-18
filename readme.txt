This is Czm's section.
本次的项目是实现对美国流域营养物质流动的hive+hbase剖析
我要实现hive查询+hbase动态存储。
核心思想：将数据集分成三份上传Linux，然后使用upload_file脚本进行文件一次一次的上传。
完成后再根据id进行查询，即可实现对每次上传的部分数据进行查询分析。
在用javaAPI形式做查询的时候，可以这样做：每次先执行一遍select max(id)，然后再用这个id指定要查询的内容where id = ? 
由于每次追加的数据一定是当前最大id，所以每次追加一次数据后执行这个java程序，查询到的就一定是我当前上传的这一部分。
然后是hbase：采用与hive集成的方式，每次追加一次数据后，就进行清空，然后插入这次的数据集指定列到hbase，然后用javaAPI导入mysql里面
Notice: Project in IDEA has Czm's username, password and hive_url, so don't copy my codes directly !
I don't write junit's codes because all HQLs are accpeted and right in dbeaver !