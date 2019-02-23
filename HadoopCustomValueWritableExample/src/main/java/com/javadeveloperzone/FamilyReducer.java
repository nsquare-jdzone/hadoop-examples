package com.javadeveloperzone;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FamilyReducer extends Reducer<IntWritable,FamilyWritable,IntWritable,FamilyWritable> {


    @Override
    protected void reduce(IntWritable key, Iterable<FamilyWritable> values, Context context) throws IOException, InterruptedException {

        FamilyWritable aggrigrateFamilyMembers = new FamilyWritable();
        for(FamilyWritable familyWritable : values){
            aggrigrateFamilyMembers.getFamilyMemberList().addAll(familyWritable.getFamilyMemberList());
            aggrigrateFamilyMembers.addTotalAge(familyWritable.getTotalAge());
        }
        aggrigrateFamilyMembers.setFamilyId(key);
        context.write(key,aggrigrateFamilyMembers);
    }
}
