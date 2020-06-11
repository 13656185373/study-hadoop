package com.auguuuste.study.flowSum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text,FLowBean,Text,FLowBean> {
    @Override
    protected void reduce(Text key, Iterable<FLowBean> values, Context context) throws IOException, InterruptedException {
        long sum_up = 0;
        long sun_down = 0;

        for (FLowBean fLowBean : values){
            sum_up += fLowBean.getUpFlow();
            sun_down += fLowBean.getDownFlow();
        }

        FLowBean result = new FLowBean(sum_up,sun_down);

        context.write(key,result);
    }
}
