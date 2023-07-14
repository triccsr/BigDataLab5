package org.GetNames;

import org.GraphFileGen.GraphFileGen;
import org.GraphFileGen.PersonWritableComparable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

public class GetNames {

    public static class GetNamesMapper extends Mapper<Object,Text,Text,Text> {

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
        private boolean is_nickname(String string) {
            return nicknameToPersons.containsKey(string);
        }
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] words=value.toString().split("(\\s|\\.|,|\"|\\?|!)+");
            HashMap<PersonWritableComparable,Integer> personCount = new HashMap<>();
            ArrayList<String> nicknameWaitList=new ArrayList<>();

            for(int i=0;i<words.length;++i){// strip off other chars
                StringBuilder newWord=new StringBuilder();
                for(int j=0;j<words[i].length();++j){
                    char c=words[i].charAt(j);
                    if((c>='a'&&c<='z')||(c>='A'&&c<='Z')||c=='-'){
//                        newWord=newWord.concat(new String(String.valueOf(c)));
                        newWord.append(c);
                    }
                }
                words[i]=newWord.toString();
            }

            for(int i=0;i<words.length;){
                PersonWritableComparable person=is_full_name(words,i);
                if(person!=null){// a full name
                    personCount.compute(person,(k,v)->(v==null)?1:(v+1));//update personCount
                    i+=person.word_number();
                    continue;
                }
                if(is_nickname(words[i])){// a nickname
                    ArrayList<PersonWritableComparable> possiblePersons=nicknameToPersons.get(words[i]);
                    if(possiblePersons.size()==1){//only one choice, so I know who the nickname refers to
                        person= possiblePersons.get(0);
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
            StringBuilder sb=new StringBuilder();
            for(PersonWritableComparable person:personCount.keySet()){
                sb.append(person.toString());
                sb.append(",");
                sb.append(personCount.getOrDefault(person, 0));
                sb.append(";");
            }
            context.write(new Text(),new Text(sb.toString()));
        }

    }

    public static class GetNamesReducer extends Reducer<Text,Text,Text,Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            for(Text t:values){
                context.write(new Text(t),new Text());
            }
        }
    }
    public static void main(String[] args) {
        try {
            Configuration conf=new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:9000");
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

            Job job = Job.getInstance(new Configuration(), "GetNames");
            job.setJarByClass(GetNames.class);


            //job.setCombinerClass(KNNCombiner.class);
            job.setReducerClass(GetNames.GetNamesReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setMapperClass(GetNames.GetNamesMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.addCacheFile(new Path(args[0]).toUri());
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
