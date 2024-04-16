package org.vinaylogics;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;

public class RightOuterJoinExample {

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // Read person file and generated tuples out of each string read
        DataSet<Tuple2<Integer, String>> personSet = env.readTextFile(params.get("input1")).map((MapFunction<String, Tuple2<Integer, String>>) value -> {
            String[] words = value.split(",");     // words = [ {1} {John}]
            return new Tuple2<>(Integer.parseInt(words[0]), words[1]);
        }).returns(new TupleTypeInfo<>(Types.INT, Types.STRING)); // personSet = tuple of (1 John)

        // Read location file and generated tuples out of each string read
        DataSet<Tuple2<Integer, String>> locationSet = env.readTextFile(params.get("input2")).map((MapFunction<String, Tuple2<Integer, String>>) value -> {
            String[] words = value.split(",");
            return new Tuple2<>(Integer.parseInt(words[0]), words[1]);
        }).returns(new TupleTypeInfo<>(Types.INT, Types.STRING)); // locationSet = tuple of (1 DC)


        // join datasets on person id
        // joined format will be <id, person_name, state>
        DataSet<Tuple3<Integer, String, String>> joined = personSet.rightOuterJoin(locationSet).where(0).equalTo(0)
                .with((JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>)
        (Tuple2<Integer,String> person,Tuple2<Integer,String> location) -> {
                    if(person == null) {
                        return new Tuple3<>(location.f0, "NULL", location.f1);
                    }
                 return new Tuple3<>(person.f0, person.f1, location.f1);
        }).returns(new TupleTypeInfo<>(Types.INT, Types.STRING, Types.STRING)); // returns tuple of (1 John DC)
        joined.writeAsCsv(params.get("output"), "\n"," ");
        env.execute("Join Example");


    }
}
