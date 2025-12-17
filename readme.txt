This is Czm's section.
本次的项目是实现对美国流域营养物质流动的hive+hbase剖析
我要实现hive查询+hbase动态存储。
核心思想：将数据集分成三份上传Linux，然后使用upload_file脚本进行文件一次一次的上传。
完成后再根据id进行查询，即可实现对每次上传的部分数据进行查询分析。
Notice: Project in IDEA has Czm's username, password and hive_url, so don't copy my codes directly !
I don't write junit's codes because all HQLs are accpeted and right in dbeaver !