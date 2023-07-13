package org.GraphFileGen;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class PersonWritableComparable implements WritableComparable {

    private int nameWordNum;
    private String[] fullName;

    public PersonWritableComparable(){
        this.nameWordNum=0;
        this.fullName=null;
    }
    public PersonWritableComparable(String fullName){
        this.fullName=fullName.split("\\s+");
        this.nameWordNum=fullName.length();
    }
    public PersonWritableComparable(String[] argName){
        this.nameWordNum= argName.length;
        this.fullName=argName;
    }
    public PersonWritableComparable clone(){
        return new PersonWritableComparable(fullName);
    }
    public String[] get(){
        return fullName;
    }
    public int word_number(){
        return nameWordNum;
    }

    @Override
    public String toString(){
        StringBuilder nameStrBuilder=new StringBuilder();
        for (String part:fullName){
            StringBuilder p=new StringBuilder();
            for(int i=0;i<part.length();++i){
                if(part.charAt(i)!=0){
                    p.append(part.charAt(i));
                }
            }
            if(nameStrBuilder.length()==0){
                nameStrBuilder.append(p);
            }
            else{
                nameStrBuilder.append(' ');
                nameStrBuilder.append(p);
            }
        }
        return nameStrBuilder.toString();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(nameWordNum);
        dataOutput.writeChars(this.toString()+"\n");
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        nameWordNum=dataInput.readInt();
        String nameStr=dataInput.readLine();
        fullName=nameStr.split("\\s+");
        assert nameWordNum==fullName.length;
    }

    @Override
    public int compareTo(Object o) {
        if(o instanceof PersonWritableComparable){
//            PersonWritableComparable p=(PersonWritableComparable) o;
//            if(nameWordNum!=p.nameWordNum){
//                return (nameWordNum<p.nameWordNum)?(-1):1;
//            }
//            for(int i=0;i<nameWordNum;++i){
//                int cmpRes=fullName[i].compareTo(p.fullName[i]);
//                if(cmpRes!=0)return cmpRes;
//            }
//            return 0;
            return this.toString().compareTo(o.toString());
        }
        assert false;
        return 0;
    }

    public int hashCode(){
        return Arrays.hashCode(fullName);
    }

    @Override
    public boolean equals(Object obj) {
        return (this.compareTo(obj)==0);
    }
}
