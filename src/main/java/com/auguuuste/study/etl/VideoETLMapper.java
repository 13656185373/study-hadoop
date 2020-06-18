package com.auguuuste.study.etl;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: study-hadoop
 * @description:
 * @author: chenteng
 * @create: 2020-06-18 10:13
 **/
public class VideoETLMapper extends Mapper<Object, Text, NullWritable, Text> {
    Text text = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String etlString = ETLUtil.oriString2ETLString(value.toString());

        if (StringUtils.isBlank(etlString)) {
            return;
        }

        text.set(etlString);
        context.write(NullWritable.get(), text);
    }
}

