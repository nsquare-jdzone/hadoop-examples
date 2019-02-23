package com.javadeveloperzone;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FamilyWritable implements Writable {

    private IntWritable familyId;
    private IntWritable totalAge;
    private List<Text> familyMemberList;

    //default constructor for (de)serialization
    public FamilyWritable() {
        familyId = new IntWritable(0);
        familyMemberList = new ArrayList<Text>();
        totalAge = new IntWritable(0);
    }

    public void write(DataOutput dataOutput) throws IOException {
        familyId.write(dataOutput); //write familyId
        totalAge.write(dataOutput); //write totalAge
        dataOutput.writeInt(familyMemberList.size());  //write size of list

        for(int index=0;index<familyMemberList.size();index++){
            familyMemberList.get(index).write(dataOutput); //write all the value of list
        }
    }


    public void readFields(DataInput dataInput) throws IOException {
        familyId.readFields(dataInput); //read familyId
        totalAge.readFields(dataInput); //read totalAge
        int size = dataInput.readInt(); //read size of list
        familyMemberList = new ArrayList<Text>(size);
        for(int index=0;index<size;index++){ //read all the values of list
            Text text = new Text();
            text.readFields(dataInput);
            familyMemberList.add(text);
        }
    }


    public IntWritable getTotalAge() {
        return totalAge;
    }

    public void setTotalAge(IntWritable totalAge) {
        this.totalAge = totalAge;
    }

    public IntWritable getFamilyId() {
        return familyId;
    }

    public void setFamilyId(IntWritable familyId) {
        this.familyId = familyId;
    }

    public List<Text> getFamilyMemberList() {
        return familyMemberList;
    }

    public void setFamilyMemberList(List<Text> familyMemberList) {
        this.familyMemberList = familyMemberList;
    }

    public FamilyWritable(IntWritable familyId, List<Text> familyMemberList) {
        this.familyId = familyId;
        this.familyMemberList = familyMemberList;
    }

    public void addFamilyMember(Text familyMember,int age){
        this.familyMemberList.add(familyMember);
        this.totalAge.set(this.totalAge.get()+age);
    }

    public void addTotalAge(IntWritable totalAge) {
        this.totalAge.set(this.totalAge.get()+totalAge.get());
    }

    @Override
    public String toString() {
        //average age, family member 1, family member 2... family member n
        return (float)totalAge.get()/ familyMemberList.size()+","+familyMemberList.toString()
                .replace("[","")
                .replace("]","");
    }
}
