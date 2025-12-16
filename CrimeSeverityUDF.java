import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

@Description(
    name = "crime_severity",
    value = "_FUNC_(crime_name) - Returns the severity level of a crime based on its name",
    extended = "Example:\n" +
    "  SELECT _FUNC_(crime_name1) FROM crime_incidents;\n"
)
public class CrimeSeverityUDF extends UDF {
    
    // 定义犯罪严重程度映射
    public Text evaluate(Text crimeName) {
        if (crimeName == null) {
            return new Text("UNKNOWN");
        }
        
        String name = crimeName.toString().toLowerCase();
        Text result = new Text();
        
        // 严重犯罪
        if (name.contains("homicide") || name.contains("murder") || name.contains("manslaughter") ||
            name.contains("rape") || name.contains("sexual assault") || name.contains("robbery") ||
            name.contains("armed robbery") || name.contains("aggravated assault") ||
            name.contains("kidnapping") || name.contains("abduction")) {
            result.set("SEVERE");
        }
        // 中等严重犯罪
        else if (name.contains("burglary") || name.contains("theft") || name.contains("larceny") ||
                 name.contains("auto theft") || name.contains("motor vehicle theft") ||
                 name.contains("arson") || name.contains("vandalism") || name.contains("criminal mischief") ||
                 name.contains("drug") || name.contains("narcotic") || name.contains("weapon")) {
            result.set("MODERATE");
        }
        // 轻微犯罪
        else if (name.contains("trespassing") || name.contains("disorderly conduct") ||
                 name.contains("loitering") || name.contains("public intoxication") ||
                 name.contains("traffic") || name.contains("driving under the influence") ||
                 name.contains("dui") || name.contains("misdemeanor")) {
            result.set("MINOR");
        }
        // 未知类型
        else {
            result.set("UNKNOWN");
        }
        
        return result;
    }
}