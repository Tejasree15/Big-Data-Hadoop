package trainings.tact.mapJoin;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapJoinMapper extends
  	Mapper<LongWritable, Text, Text, Text> {

	HashMap<String, String> DepartmentMap = new HashMap<String, String>();
	BufferedReader brReader;
	
	Text txtMapOutputKey = new Text("");
	Text txtMapOutputValue = new Text("");

	enum MYCOUNTER {
		RECORD_COUNT, FILE_EXISTS, FILE_NOT_FOUND, SOME_OTHER_ERROR
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context
				.getConfiguration());

		for (Path eachPath : cacheFilesLocal) {
			if (eachPath.getName().toString().trim().equals("Departments")) {
				context.getCounter(MYCOUNTER.FILE_EXISTS).increment(1);
				loadDepartmentsHashMap(eachPath, context);
			}
		}

	}

	private void loadDepartmentsHashMap(Path filePath, Context context)
			throws IOException {
		System.out.println("inside load hashmap");
		String strLineRead = "";

		try {
			brReader = new BufferedReader(new FileReader(filePath.toString()));

			// Read each line, split and load to HashMap
			while ((strLineRead = brReader.readLine()) != null) {
				String deptFieldArray[] = strLineRead.split(",");
				String deptId = deptFieldArray[0].trim();
				String deptName = deptFieldArray[1].trim();
				System.out.println(deptId +" "+deptName);
				DepartmentMap.put(deptId,deptName);
			}
			System.out.println("hashmap size:" +DepartmentMap.size());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			context.getCounter(MYCOUNTER.FILE_NOT_FOUND).increment(1);
		} catch (IOException e) {
			context.getCounter(MYCOUNTER.SOME_OTHER_ERROR).increment(1);
			e.printStackTrace();
		}finally {
			if (brReader != null) {
				brReader.close();

			}

		}
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String strDeptName = "";

		context.getCounter(MYCOUNTER.RECORD_COUNT).increment(1);

		if (value.toString().length() > 0) {
			String arrEmpAttributes[] = value.toString().split(",");

			try {
				strDeptName = DepartmentMap.get(arrEmpAttributes[6].toString());
			} finally {
				//strDeptName = ((strDeptName.equals(null) || strDeptName.equals("")) ? "NOT-FOUND" : strDeptName);
			}

			txtMapOutputKey.set(arrEmpAttributes[0].toString());

			txtMapOutputValue.set(arrEmpAttributes[1].toString() + "\t"
					+ arrEmpAttributes[2].toString() + "\t"
					+ arrEmpAttributes[3].toString() + "\t"
					+ arrEmpAttributes[4].toString() + "\t"
					+ arrEmpAttributes[5].toString() + "\t"
					+ arrEmpAttributes[6].toString() + "\t" + strDeptName);

		}
		context.write(txtMapOutputKey, txtMapOutputValue);
	
	}
}