package org.GraphFileGen;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.util.*;

import static jdk.nashorn.internal.objects.NativeMath.min;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class GraphFileGen {

    private class FullName {
        private String firstNameString;
        private String surnameString;
        String fullNameString;
        String get(){
            return fullNameString;
        }

        public FullName(String fullName){
            fullNameString=fullName;
            String[] s=fullNameString.split("\\s+");// split by whitespace
            assert s.length==2;
            firstNameString=s[0];
            surnameString=s[1];
        }
        public FullName(String firstName,String surname){
            firstNameString=firstName;
            surnameString=surname;
            fullNameString=firstNameString+" "+surnameString;
        }
    }
    private static class NeighborWritable implements Writable {
        PersonWritableComparable to;
        long count;

        public NeighborWritable(){
            to=new PersonWritableComparable();
            count=0;
        }

        public NeighborWritable(PersonWritableComparable argTo,long argCount){
            to=argTo;
            count=argCount;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            to.write(dataOutput);
            dataOutput.writeLong(count);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            to.readFields(dataInput);
            count=dataInput.readLong();
        }
    }
    public static class GraphFileGenMapper extends Mapper<Object,Text,PersonWritableComparable,NeighborWritable>{

        private HashSet<PersonWritableComparable> persons;
        private HashMap<String, ArrayList<PersonWritableComparable>> nicknameToPersons;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 将 NameList 装入本地的内存数据中
            try {
                Configuration conf=context.getConfiguration();
                URI[] cacheFiles= DistributedCache.getCacheFiles(conf);
                //System.out.println(cacheFiles);
                if (cacheFiles != null && cacheFiles.length > 0) {
                    String line;

                    try (BufferedReader joinReader = new BufferedReader(new FileReader(cacheFiles[0].getPath()))) {
                        ArrayList<String[]> allNames = new ArrayList<>();
                        while ((line = joinReader.readLine()) != null) {
                            String[] words = line.split("\\s+");
                            allNames.add(words);
                        }

                        //init instance attributes
                        persons=new HashSet<>();
                        nicknameToPersons=new HashMap<>();

                        //sort allNames(no matter full or part) in descending order of length
                        allNames.sort(Comparator.comparingInt((String[] a) -> -a.length));

                        HashSet<String> namePartSet=new HashSet<>();
                        for (String[] name : allNames) {
                            if (name.length == 1 && namePartSet.contains(name[0])) {//is not a full name
                                if(!nicknameToPersons.containsKey(name[0])){// add it to nickname map
                                    nicknameToPersons.put(name[0],new ArrayList<>());
                                }
                                // do nothing
                            }
                            else{// is a full name
                                persons.add(new PersonWritableComparable(name));// new person
                                namePartSet.addAll(Arrays.asList(name));
                            }
                        }

                        for(String part:nicknameToPersons.keySet()){//all nicknames (contains only 1 word)
                            ArrayList<PersonWritableComparable> arrayList=nicknameToPersons.get(part);
                            assert arrayList!=null;
                            for(PersonWritableComparable person: persons){//for all full names
                                String[] fullName=person.get();
                                boolean contains=false;
                                for(String word:fullName){
                                    if(word.equals(part)){
                                        contains=true;
                                        break;
                                    }
                                }
                                if(contains){// if a person's full name contains this nickname, add
                                    arrayList.add(person);
                                }
                            }

                        }

                    }
                }
            } catch (IOException e) {
                System.err.println("Exception reading DistributedCache: "+e);
            }
        }

        private PersonWritableComparable is_full_name(String[] strings, int startIndex){
            assert strings.length>0;
            for(int len=3;len>=1;--len){
                if(startIndex+len>strings.length)continue;
                String[] temps=Arrays.copyOfRange(strings,startIndex,startIndex+len);
                PersonWritableComparable tmpPerson=new PersonWritableComparable(temps);
                if(persons.contains(tmpPerson)){
                    return tmpPerson;
                }
            }
            return null;
        }
        private boolean is_nickname(String string){
            return nicknameToPersons.containsKey(string);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] words=value.toString().split("(\\s|\\.|,|\"|\\?|!)+");
            HashMap<PersonWritableComparable,Integer> personCount = new HashMap<>();
            ArrayList<String> nicknameWaitList=new ArrayList<>();

            for(int i=0;i<words.length;++i){// strip off other chars
                String newWord= "";
                for(int j=0;j<words[i].length();++j){
                    char c=words[i].charAt(j);
                    if((c>='a'&&c<='z')||(c>='A'&&c<='Z')||c=='-'){
                        newWord=newWord.concat(new String(String.valueOf(c)));
                    }
                }
                words[i]=newWord;
            }

            for(int i=0;i<words.length;){
                PersonWritableComparable person=is_full_name(words,i);
                if(person!=null){// a full name
//                    int oldCount=personCount.getOrDefault(person,0);
//                    personCount.put(person,oldCount+1);
                    personCount.compute(person,(k,v)->(v==null)?1:(v+1));//update personCount
                    i+=person.word_number();
                    continue;
                }
                if(is_nickname(words[i])){// a nickname
                    ArrayList<PersonWritableComparable> possiblePersons=nicknameToPersons.get(words[i]);
                    if(possiblePersons.size()==1){//only one choice
                        person= possiblePersons.get(0);
//                        int oldCount=personCount.getOrDefault(person,0);//update personCount
//                        personCount.put(person,oldCount+1);
                        personCount.compute(person,(k,v)->(v==null)?1:(v+1));
                    }
                    else if(possiblePersons.size()>1){//more than one possible person, throw into wl
                        nicknameWaitList.add(words[i]);
                    }
                    i+=1;
                    continue;
                }
                i+=1;//not a name
            }
            for(String nickname:nicknameWaitList){//do with waiting nicknames
                PersonWritableComparable finalPerson=null;
                for(PersonWritableComparable possiblePerson:nicknameToPersons.get(nickname)){
                    if(personCount.getOrDefault(possiblePerson,0)>0){// this person appears >=1 times in this line
                        if(finalPerson!=null){// 2 different person with the same nickname, cannot figure out which one the nickname refers to.
                            finalPerson=null;
                            break;
                        }
                        else{
                            finalPerson=possiblePerson;
                        }
                    }
                }
                if(finalPerson!=null){
                    personCount.compute(finalPerson,(k,v)->(v==null)?1:(v+1));
                }
            }
            for(PersonWritableComparable src: personCount.keySet()){
                for(PersonWritableComparable dst:personCount.keySet()){
                    if(dst.equals(src))continue;
                    context.write(src,new NeighborWritable(dst,(long)personCount.get(src)*(long)personCount.get(dst)));
                }
            }
        }
    }

    public static class EdgeWritable implements Writable{
        PersonWritableComparable to;
        double weight;

        public EdgeWritable(){
            to=null;
            weight=0.0;
        }
        public EdgeWritable(PersonWritableComparable argTo,double argWeight){
            to=argTo;
            weight=argWeight;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            to.write(dataOutput);
            dataOutput.writeDouble(weight);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            to.readFields(dataInput);
            weight=dataInput.readDouble();
        }
    }
    public static class GraphFileGenReducer extends Reducer<PersonWritableComparable,NeighborWritable,Text,Text>{
        @Override
        public void reduce(PersonWritableComparable key,Iterable<NeighborWritable> values, Context context) throws IOException, InterruptedException{
            HashMap<PersonWritableComparable,Long> count=new HashMap<>();
            long sum=0;
            for(NeighborWritable value:values){
                sum+=value.count;
                count.compute(value.to.clone(),(k,v)->(v==null)?value.count:(v+value.count));
            }
//            for(PersonWritableComparable to:count.keySet()){
//                context.write(key,new EdgeWritable(to,((double)count.getOrDefault(to,0l))/(double)sum));
//            }
            //ArrayList<String> dstList=new ArrayList<>();
            StringBuilder sb=new StringBuilder("[");
            boolean firstString=true;
            for(PersonWritableComparable to:count.keySet()) {
                String str = String.format("%s,%.16f", to.toString(), ((double) count.getOrDefault(to, 0l)) / (double) sum);
                //dstList.add(str);
                if (!firstString) {
                    sb.append('|');
                }
                firstString = false;
                sb.append(str);
            }
            sb.append(']');
            context.write(new Text(key.toString()),new Text(sb.toString()));
        }
    }

    public static void main(String[] args) {
        try {
            Configuration conf=new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:9000");
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

            Job job = Job.getInstance(new Configuration(), "GraphFileGen");
            job.setJarByClass(GraphFileGen.class);


            //job.setCombinerClass(KNNCombiner.class);
            job.setReducerClass(GraphFileGenReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setMapperClass(GraphFileGenMapper.class);
            job.setMapOutputKeyClass(PersonWritableComparable.class);
            job.setMapOutputValueClass(NeighborWritable.class);

            job.addCacheFile(new Path(args[0]).toUri());
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}