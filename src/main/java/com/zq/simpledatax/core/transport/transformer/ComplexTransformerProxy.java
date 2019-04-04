package com.zq.simpledatax.core.transport.transformer;

import java.util.Map;

import com.zq.simpledatax.common.element.Record;
import com.zq.simpledatax.transformer.ComplexTransformer;
import com.zq.simpledatax.transformer.Transformer;

/**
 * no comments.
 * Created by liqiang on 16/3/8.
 */
public class ComplexTransformerProxy extends ComplexTransformer {
    private Transformer realTransformer;

    public ComplexTransformerProxy(Transformer transformer) {
        setTransformerName(transformer.getTransformerName());
        this.realTransformer = transformer;
    }

    @Override
    public Record evaluate(Record record, Map<String, Object> tContext, Object... paras) {
        return this.realTransformer.evaluate(record, paras);
    }

    public Transformer getRealTransformer() {
        return realTransformer;
    }
}
