package org.vinaylogics;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

public class MyJoinFunction implements ResultTypeQueryable<Tuple3<Integer, String, String>>, JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>> {
    @Override
    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> person, Tuple2<Integer, String> location) throws Exception {
        return new Tuple3<>(person.f0, person.f1, location.f1);
    }

    @Override
    public TypeInformation<Tuple3<Integer, String, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<>() {
            @Override
            public TypeInformation<Tuple3<Integer, String, String>> getTypeInfo() {
                return super.getTypeInfo();
            }
        });
    }
}
