package wise2012;

import java.io.File;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class citeseer {

	
//////////////////////////////////////////////////////////////////////////////////////////////////
////// COMPUTING SCIENTOMETRICS WITH MAPREDUCE: METHOD 1 /////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////
	
	// Plain Map Function, no combiners, tuples <author, pair [paperID, score]> output
	public static class Map1 extends Mapper<LongWritable, Text, Text, MapOutputValue> {
		private Text word = new Text();
		private int s = 0, e = 0, s1 = 0, e1 = 0;

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// Convert the Input Map-value into a string (the input file records are split by line feeds)
			String line = value.toString();
			String refXML = new String();
			String refID = new String();
			String ReferenceAuthors = new String();

			int refIDint;

			// Get the references of the paper
			s = 0; e = 0;
			while (s != -1) {

				// Get the XML text of each reference
				s = line.indexOf("<citation id=\"", s);
				if (s > 0) {
					s += 14;
					e = line.indexOf("</citation>", s);
					refXML = line.substring(s, e);

					// Retrieve the ID (clusterID) of the reference 
					s1 = 0; e1 = 0;
					s1 = refXML.indexOf("<clusterid>");

					if (s1 > 0) {
						s1 += 11;
						e1 = refXML.indexOf("</clusterid>", s1);
						refID = refXML.substring(s1, e1);
						refIDint = Integer.parseInt(refID);

						// Retrieve the authors of the reference
						s1 = 0; e1 = 0;
						s1 = refXML.indexOf("<authors>");
						if (s1 > 0) {
							s1 += 9;
							e1 = refXML.indexOf("</authors>", s1);
							ReferenceAuthors = refXML.substring(s1, e1);
								
							// Split the ReferenceAuthors string and get the authors
							String ReferenceAuthorsArray[] = ReferenceAuthors.split(",");
							for (String a : ReferenceAuthorsArray) {
								if (a.length() > 0 && refIDint > 0) {
									MapOutputValue citationScore = new MapOutputValue(refIDint, 1.0f);
									word.set(a);
									context.write(word, citationScore);
								}
							}
						}
					}
				}
			}
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			System.out.println("Cleanup called!");
		}
	}

	// The Reduce function tuned for compatibility with Map1 
	public static class Reduce1 extends Reducer<Text, MapOutputValue, Text, IntWritable> {

		public void reduce(Text key, Iterable<MapOutputValue> values, Context context) throws IOException, InterruptedException {

			Hashtable<Integer, Float> citations = new Hashtable<Integer, Float>();
			int metric = 0, entry = 0;
			float score = 0.0f;

			for (MapOutputValue val : values) {
//				System.out.println("Key " + key + ". Paper " + val.get_paperID());
				if (citations.get(val.get_paperID()) == null) {
					citations.put(val.get_paperID(), val.get_paperScore());
				} else {
					citations.put(val.get_paperID(),
							val.get_paperScore() + citations.get(val.get_paperID()).floatValue());
				}
			}

/*			Iterator<Integer> it = citations.keySet().iterator();
			System.out.println("Author: " + key);
			while (it.hasNext()) {
				int element = (Integer)it.next();
				System.out.println("Paper: " + element + ". Score: " + citations.get(element));
			} */

			Vector<Float> v = new Vector<Float>(citations.values());
			Collections.sort(v, Collections.reverseOrder());
			Iterator<Float> float_it = v.iterator();

			while (float_it.hasNext()) {
				entry++;
				score = (Float)float_it.next();
 //System.out.println("Entry = " + entry + "Sorted Score = " + score);
				if (score >= entry) {
					metric++;
				} else {
					break;
				}
			}

			context.write(key, new IntWritable(metric));
		}
	}



	
	
//////////////////////////////////////////////////////////////////////////////////////////////////
//////COMPUTING SCIENTOMETRICS WITH MAPREDUCE: METHOD 2 //////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////


	// Plain Map Function, no combiners, tuples <[author, paperID], score> output
	public static class Map2 extends Mapper<LongWritable, Text, MapOutputKey, FloatWritable> {
		private int s = 0, e = 0, s1 = 0, e1 = 0;
		private FloatWritable score = new FloatWritable(1.0f);

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// Convert the Input Map-value into a string (the input file records are split by line feeds)
			String line = value.toString();
			String refXML = new String();
			String refID = new String();
			String ReferenceAuthors = new String();

			int refIDint;

			// Get the references of the paper
			s = 0; e = 0;
			while (s != -1) {

				// Get the XML text of each reference
				s = line.indexOf("<citation id=\"", s);
				if (s > 0) {
					s += 14;
					e = line.indexOf("</citation>", s);
					refXML = line.substring(s, e);

					// Retrieve the ID (clusterID) of the reference 
					s1 = 0; e1 = 0;
					s1 = refXML.indexOf("<clusterid>");

					if (s1 > 0) {
						s1 += 11;
						e1 = refXML.indexOf("</clusterid>", s1);
						refID = refXML.substring(s1, e1);
						refIDint = Integer.parseInt(refID);

						// Retrieve the authors of the reference
						s1 = 0; e1 = 0;
						s1 = refXML.indexOf("<authors>");
						if (s1 > 0) {
							s1 += 9;
							e1 = refXML.indexOf("</authors>", s1);
							ReferenceAuthors = refXML.substring(s1, e1);
								
							// Split the ReferenceAuthors string and get the authors
							String ReferenceAuthorsArray[] = ReferenceAuthors.split(",");
							for (String a : ReferenceAuthorsArray) {
								if (a.length() > 0 && refIDint > 0) {
									MapOutputKey custom_key = new MapOutputKey(a, refIDint);
									context.write(custom_key, score);
								}
							}
						}
					}
				}
			}
		}
	}

	// The Reduce function tuned for compatibility with Map1 
	public static class Reduce2 extends Reducer<MapOutputKey, FloatWritable, Text, IntWritable> {
		int reduce_start = 1, npapers = 0;
		float score = 0.0f;

		Text previous_author = new Text();
		int previous_paperID = 0;

		// Store the paper scores into a vector
		Vector<Float> v = new Vector<Float>();

		public void reduce(MapOutputKey key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {

			int entry = 0;
			int metric = 0;

			for (FloatWritable val : values) {
//				System.out.println("Previous Author: " + previous_author + ". Current Author: " + key.get_author());
				if (key.get_author().equals(previous_author)) {
					if (key.get_paperID() == previous_paperID) {
//						System.out.println("Equal npapers = " + npapers + ". Previous Paper: " + previous_paperID + ". Current PaperID: " + key.get_paperID());
						v.set(npapers, v.get(npapers) + val.get());
					} else {
//						System.out.println("Not Equal npapers = " + npapers + ". Previous Paper: " + previous_paperID + ". Current PaperID: " + key.get_paperID());
						v.add(val.get());
						npapers++;
//						System.out.println("Insert paper at index " + npapers);
						previous_paperID = key.get_paperID();
					}
				} else {
					// Emit the previous author
					if (reduce_start == 0) {
						Collections.sort(v, Collections.reverseOrder());
//						System.out.println("Vector Size: " + v.size());
						Iterator<Float> float_it = v.iterator();
						entry = 0;
						metric = 0;

						while (float_it.hasNext()) {
							entry++;
							score = (Float)float_it.next();
// System.out.println(previous_author + "Entry = " + entry + "Sorted Score = " + score);
							if (score >= entry) {
								metric++;
							} else {
								break;
							}
						}
						context.write(previous_author, new IntWritable(metric));
					}

					// Initialize the data for the current author
					v.clear();
					previous_author.set(key.get_author());
					previous_paperID = key.get_paperID();

					v.add(val.get());
					npapers = 0;
//					System.out.println("New Author " + key.get_author() + ". Insert paper at index " + npapers);
					reduce_start = 0;
				}
			}
		}
	}

	
	

	
//////////////////////////////////////////////////////////////////////////////////////////////////
//////COMPUTING SCIENTOMETRICS WITH MAPREDUCE: METHOD 1-C (Method 1 with in-Mapper Combiner) /////
//////////////////////////////////////////////////////////////////////////////////////////////////

	// Plain Map Function, no combiners, tuples <author, pair [paperID, score]> output
	public static class Map3 extends Mapper<LongWritable, Text, Text, MapOutputValueCombiner> {
		private Text word = new Text();
		private int s = 0, e = 0, s1 = 0, e1 = 0;

		Hashtable<String, MapOutputValueCombiner> authors_papers =
			new Hashtable<String, MapOutputValueCombiner>();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// Convert the Input Map-value into a string (the input file records are split by line feeds)
			String line = value.toString();
			String refXML = new String();
			String refID = new String();
			String ReferenceAuthors = new String();

			int refIDint;

			// Get the references of the paper
			s = 0; e = 0;
			while (s != -1) {

				// Get the XML text of each reference
				s = line.indexOf("<citation id=\"", s);
				if (s > 0) {
					s += 14;
					e = line.indexOf("</citation>", s);
					refXML = line.substring(s, e);

					// Retrieve the ID (clusterID) of the reference 
					s1 = 0; e1 = 0;
					s1 = refXML.indexOf("<clusterid>");

					if (s1 > 0) {
						s1 += 11;
						e1 = refXML.indexOf("</clusterid>", s1);
						refID = refXML.substring(s1, e1);
						refIDint = Integer.parseInt(refID);

						// Retrieve the authors of the reference
						s1 = 0; e1 = 0;
						s1 = refXML.indexOf("<authors>");
						if (s1 > 0) {
							s1 += 9;
							e1 = refXML.indexOf("</authors>", s1);
							ReferenceAuthors = refXML.substring(s1, e1);
								
							// Split the ReferenceAuthors string and get the authors
							String ReferenceAuthorsArray[] = ReferenceAuthors.split(",");
							for (String a : ReferenceAuthorsArray) {
								if (a.length() > 0 && refIDint > 0) {
									if (authors_papers.containsKey(a)) {
										MapOutputValueCombiner movc = authors_papers.get(a);
//										System.out.println("Author: " + a + ". Before Insertion " + movc.toString());
										movc.insert(refIDint, 1.0f);
//										System.out.println("Author: " + a + ". After Insertion: " + movc.toString());
										authors_papers.put(a, movc);
									}else {
										MapOutputValueCombiner movc =
											new MapOutputValueCombiner(refIDint, 1.0f);
										authors_papers.put(a, movc);
									}
								}
							}
						}
					}
				}
			}
		}

		// Mapper close method. At the end of the map task flush out the Hashtable
		public void cleanup(Context context) throws IOException, InterruptedException {
			Set<String> key_set = authors_papers.keySet();
			Iterator<String> itr = key_set.iterator();
			String str;

			while (itr.hasNext()) {
				str = itr.next();
				word = new Text(str);
				context.write(word, authors_papers.get(str));
			}
		}
	}

	// The Reduce function tuned for compatibility with Map1 
	public static class Reduce3 extends Reducer<Text, MapOutputValueCombiner, Text, IntWritable> {

		public void reduce(Text key, Iterable<MapOutputValueCombiner> values, Context context) throws IOException, InterruptedException {

			Hashtable<Integer, Float> citations = new Hashtable<Integer, Float>(); 
			Vector<MapOutputValue> vec = new Vector<MapOutputValue>();

			int metric = 0, entry = 0, num_vector_values = 0;
			float score = 0.0f;

			for (MapOutputValueCombiner val : values) {
				vec = val.get_values();
				num_vector_values = vec.size();
//				System.out.println("Key " + key + " (" + num_vector_values + "). Papers " + vec);

				for (int i = 0; i < num_vector_values; i++) {
					if (citations.get(vec.elementAt(i).get_paperID()) == null) {
						citations.put(vec.elementAt(i).get_paperID(), vec.elementAt(i).get_paperScore());
					} else {
						citations.put(vec.elementAt(i).get_paperID(),
							vec.elementAt(i).get_paperScore() +
							citations.get(vec.elementAt(i).get_paperID()).floatValue());
					}
				}
				vec.clear();
			}

			Vector<Float> v = new Vector<Float>(citations.values());
			Collections.sort(v, Collections.reverseOrder());
			Iterator<Float> float_it = v.iterator();

			while (float_it.hasNext()) {
				entry++;
				score = (Float)float_it.next();
 //System.out.println("Entry = " + entry + "Sorted Score = " + score);
				if (score >= entry) {
					metric++;
				} else {
					break;
				}
			}

			context.write(key, new IntWritable(metric));
		}
	}

	
	
	
	
//////////////////////////////////////////////////////////////////////////////////////////////////
//////COMPUTING SCIENTOMETRICS WITH MAPREDUCE: METHOD 2-C (Method 2 with in-Mapper Combiner) /////
//////////////////////////////////////////////////////////////////////////////////////////////////

	// Plain Map Function, no combiners, tuples <[author, paperID], score> output
	public static class Map4 extends Mapper<LongWritable, Text, MapOutputKey, FloatWritable> {
		private int s = 0, e = 0, s1 = 0, e1 = 0;

		Hashtable<MapOutputKey, Float> authors_papers = new Hashtable<MapOutputKey, Float>();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// Convert the Input Map-value into a string (the input file records are split by line feeds)
			String line = value.toString();
			String refXML = new String();
			String refID = new String();
			String ReferenceAuthors = new String();

			int refIDint;

			// Get the references of the paper
			s = 0; e = 0;
			while (s != -1) {

				// Get the XML text of each reference
				s = line.indexOf("<citation id=\"", s);
				if (s > 0) {
					s += 14;
					e = line.indexOf("</citation>", s);
					refXML = line.substring(s, e);

					// Retrieve the ID (clusterID) of the reference 
					s1 = 0; e1 = 0;
					s1 = refXML.indexOf("<clusterid>");

					if (s1 > 0) {
						s1 += 11;
						e1 = refXML.indexOf("</clusterid>", s1);
						refID = refXML.substring(s1, e1);
						refIDint = Integer.parseInt(refID);

						// Retrieve the authors of the reference
						s1 = 0; e1 = 0;
						s1 = refXML.indexOf("<authors>");
						if (s1 > 0) {
							s1 += 9;
							e1 = refXML.indexOf("</authors>", s1);
							ReferenceAuthors = refXML.substring(s1, e1);
								
							// Split the ReferenceAuthors string and get the authors
							String ReferenceAuthorsArray[] = ReferenceAuthors.split(",");
							for (String a : ReferenceAuthorsArray) {
								if (a.length() > 0 && refIDint > 0) {
									MapOutputKey custom_key = new MapOutputKey(a, refIDint);
									if (authors_papers.containsKey(custom_key)) {
										authors_papers.put(custom_key,
												1.0f + authors_papers.get(custom_key).floatValue());
									} else {
										authors_papers.put(custom_key, 1.0f);
									}
								}
							}
						}
					}
				}
			}
		}

		// Mapper close method. At the end of the map task flush out the Hashtable
		public void cleanup(Context context) throws IOException, InterruptedException {
			Set<MapOutputKey> key_set = authors_papers.keySet();
			Iterator<MapOutputKey> itr = key_set.iterator();
			MapOutputKey str;

			while (itr.hasNext()) {
				str = itr.next();
				context.write(str, new FloatWritable(authors_papers.get(str).floatValue()));
			}
		}
	}

	// The Reduce function tuned for compatibility with Map1 
	public static class Reduce4 extends Reducer<MapOutputKey, FloatWritable, Text, IntWritable> {
		int reduce_start = 1, npapers = 0;
		float score = 0.0f;

		Text previous_author = new Text();
		int previous_paperID = 0;

		// Store the paper scores into a vector
		Vector<Float> v = new Vector<Float>();

		public void reduce(MapOutputKey key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {

			int entry = 0;
			int metric = 0;

			for (FloatWritable val : values) {
//				System.out.println("Previous Author: " + previous_author + ". Current Author: " + key.get_author());
				if (key.get_author().equals(previous_author)) {
					if (key.get_paperID() == previous_paperID) {
//						System.out.println("Equal npapers = " + npapers + ". Previous Paper: " + previous_paperID + ". Current PaperID: " + key.get_paperID());
						v.set(npapers, v.get(npapers) + val.get());
					} else {
//						System.out.println("Not Equal npapers = " + npapers + ". Previous Paper: " + previous_paperID + ". Current PaperID: " + key.get_paperID());
						v.add(val.get());
						npapers++;
//						System.out.println("Insert paper at index " + npapers);
						previous_paperID = key.get_paperID();
					}
				} else {
					// Emit the previous author
					if (reduce_start == 0) {
						Collections.sort(v, Collections.reverseOrder());
//						System.out.println("Vector Size: " + v.size());
						Iterator<Float> float_it = v.iterator();
						entry = 0;
						metric = 0;

						while (float_it.hasNext()) {
							entry++;
							score = (Float)float_it.next();
// System.out.println(previous_author + "Entry = " + entry + "Sorted Score = " + score);
							if (score >= entry) {
								metric++;
							} else {
								break;
							}
						}
						context.write(previous_author, new IntWritable(metric));
					}

					// Initialize the data for the current author
					v.clear();
					previous_author.set(key.get_author());
					previous_paperID = key.get_paperID();

					v.add(val.get());
					npapers = 0;
//					System.out.println("New Author " + key.get_author() + ". Insert paper at index " + npapers);
					reduce_start = 0;
				}
			}
		}
	}

	
	// MapReduce Driver
	public static void main(String[] args) throws Exception {

		// Delete Previous MapReduce Output
    	File directory = new File(args[1]);

    	if (directory.exists()) {
    		try{ 
    			delete_dir(directory);
    		} catch(IOException e) {
    			e.printStackTrace();
    			System.exit(0);
    		}
    	}

    	Job job = Job.getInstance(new Configuration());

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

    	// MapReduce Configuration for Method 1.
    	if (args[2].equals("1")) {
            job.setJarByClass(Map1.class);

        	job.setOutputKeyClass(Text.class); // Reducer Output: Key
        	job.setOutputValueClass(IntWritable.class); // Reducer Output: Value
        	job.setMapOutputKeyClass(Text.class); // Mapper Output: Key
        	job.setMapOutputValueClass(MapOutputValue.class); // Mapper Output: Value
        	job.setMapperClass(Map1.class);
        	job.setReducerClass(Reduce1.class);
    	}

    	// MapReduce Configuration for Method 2.
    	if (args[2].equals("2")) {
            job.setJarByClass(Map2.class);

        	job.setOutputKeyClass(Text.class); // Reducer Output: Key
        	job.setOutputValueClass(IntWritable.class); // Reducer Output: Value
        	job.setMapOutputKeyClass(MapOutputKey.class); // Mapper Output: Key
        	job.setMapOutputValueClass(FloatWritable.class); // Mapper Output: Value
        	job.setMapperClass(Map2.class);
        	job.setReducerClass(Reduce2.class);
    	}

    	// MapReduce Configuration for Method 1-C.
    	if (args[2].equals("1-C")) {
            job.setJarByClass(Map3.class);

    		job.setOutputKeyClass(Text.class); // Reducer Output: Key
    		job.setOutputValueClass(IntWritable.class); // Reducer Output: Value
    		job.setMapOutputKeyClass(Text.class); // Mapper Output: Key
    		job.setMapOutputValueClass(MapOutputValueCombiner.class); // Mapper Output: Value
    		job.setMapperClass(Map3.class);
    		job.setReducerClass(Reduce3.class);
    	}

    	// MapReduce Configuration for Method 2-C.
    	if (args[2].equals("2-C")) {
            job.setJarByClass(Map4.class);

        	job.setOutputKeyClass(Text.class); // Reducer Output: Key
        	job.setOutputValueClass(IntWritable.class); // Reducer Output: Value
        	job.setMapOutputKeyClass(MapOutputKey.class); // Mapper Output: Key
        	job.setMapOutputValueClass(FloatWritable.class); // Mapper Output: Value
        	job.setMapperClass(Map4.class);
        	job.setReducerClass(Reduce4.class);
    	}
    	
    	job.setInputFormatClass(TextInputFormat.class);
    	job.setOutputFormatClass(TextOutputFormat.class);

    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));

    	job.waitForCompletion(true);
	}



	// Recursive Directory Deletion; used to delete the previous MapReduce Outputs 
	public static void delete_dir(File file) throws IOException{
		if (file.isDirectory()) {
			if (file.list().length == 0) {
			   file.delete();
			   System.out.println("Directory is deleted : "  + file.getAbsolutePath());
			} else {
				String files[] = file.list();
				for (String temp : files) {
					File fileDelete = new File(file, temp);
					delete_dir(fileDelete);
				}

				if(file.list().length == 0){
					file.delete();
					System.out.println("Directory is deleted : " + file.getAbsolutePath());
				}
			}
		} else {
			file.delete();
			System.out.println("File is deleted : " + file.getAbsolutePath());
		}
	}
}
