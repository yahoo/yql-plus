/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.internal.bytecode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.yahoo.yqlplus.compiler.generate.ExactInvocation;
import com.yahoo.yqlplus.engine.internal.plan.ast.FunctionOperator;
import com.yahoo.yqlplus.engine.internal.plan.ast.PhysicalExprOperator;
import com.yahoo.yqlplus.engine.internal.plan.streams.SinkOperator;
import com.yahoo.yqlplus.engine.internal.plan.streams.StreamOperator;
import com.yahoo.yqlplus.compiler.types.BaseTypeAdapter;
import com.yahoo.yqlplus.compiler.types.ListTypeWidget;
import com.yahoo.yqlplus.language.operator.OperatorNode;

import org.objectweb.asm.Opcodes;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.Callable;

public class StreamOperatorCompilerTest extends CompilingTestBase {

    protected Callable<Object> compileStream(Object input, OperatorNode<StreamOperator> stream) throws IllegalAccessException, InvocationTargetException, IOException, InstantiationException, NoSuchMethodException, ClassNotFoundException {
        OperatorNode<PhysicalExprOperator> executedStream = OperatorNode.create(PhysicalExprOperator.STREAM_EXECUTE, constant(input), stream);
        return compileExpression(executedStream);
    }

    public static class MyRecord {
        public int ival;
        public String sval;

        public MyRecord(int ival, String sval) {
            this.ival = ival;
            this.sval = sval;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MyRecord myRecord = (MyRecord) o;

            if (ival != myRecord.ival) return false;
            return sval.equals(myRecord.sval);
        }

        @Override
        public int hashCode() {
            int result = ival;
            result = 31 * result + sval.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "MyRecord{" +
                    "ival=" + ival +
                    ", sval='" + sval + '\'' +
                    '}';
        }
    }

    @Test
    public void requireNoop() throws Exception {
        List<MyRecord> input = ImmutableList.of(new MyRecord(1, "one"), new MyRecord(2, "two"));
        Callable<Object> invoker = compileStream(input, accumulate());
        Assert.assertEquals(input, invoker.call());
    }

    @Test
    public void requireNoopDistinct() throws Exception {
        List<MyRecord> input = ImmutableList.of(new MyRecord(1, "one"), new MyRecord(2, "two"));
        Callable<Object> invoker = compileStream(input, OperatorNode.create(StreamOperator.DISTINCT, accumulate()));
        Assert.assertEquals(input, invoker.call());
    }

    @Test
    public void requireDistinct() throws Exception {
        List<MyRecord> input = ImmutableList.of(new MyRecord(1, "one"), new MyRecord(2, "two"), new MyRecord(1, "one"));
        Callable<Object> invoker = compileStream(input, OperatorNode.create(StreamOperator.DISTINCT, accumulate()));
        Assert.assertEquals(ImmutableList.of(new MyRecord(1, "one"), new MyRecord(2, "two")), invoker.call());
    }

    @Test
    public void requireFlatten() throws Exception {
        List<List<MyRecord>> input = ImmutableList.of(
                ImmutableList.of(new MyRecord(1, "one")),
                ImmutableList.of(new MyRecord(2, "two"), new MyRecord(3, "three")),
                ImmutableList.of(),
                ImmutableList.of(new MyRecord(4, "four"))
        );
        Callable<Object> invoker = compileStream(input, OperatorNode.create(StreamOperator.FLATTEN, accumulate()));
        Assert.assertEquals(ImmutableList.of(new MyRecord(1, "one"), new MyRecord(2, "two"), new MyRecord(3, "three"), new MyRecord(4, "four")), invoker.call());
    }

    @Test
    public void requireGroupbyKeys() throws Exception {
        List<MyRecord> input = ImmutableList.of(new MyRecord(1, "a"), new MyRecord(1, "b"), new MyRecord(2, "c"));
        Callable<Object> invoker = compileStream(input, OperatorNode.create(StreamOperator.GROUPBY, accumulate(),
                OperatorNode.create(FunctionOperator.FUNCTION, ImmutableList.of("$row"), OperatorNode.create(PhysicalExprOperator.PROPREF, OperatorNode.create(PhysicalExprOperator.LOCAL, "$row"), "ival")),
                OperatorNode.create(FunctionOperator.FUNCTION, ImmutableList.of("$key", "$rows"), OperatorNode.create(PhysicalExprOperator.LOCAL, "$key"))
        ));
        Assert.assertEquals(ImmutableList.of(1, 2), invoker.call());
    }

    public static MyRecord aggregate(String key, List<MyRecord> inputs) {
        int val = 0;
        for (MyRecord record : inputs) {
            val += record.ival;
        }
        return new MyRecord(val, key);
    }

    @Test
    public void requireGroupbyAggregate() throws Exception {
        List<MyRecord> input = ImmutableList.of(new MyRecord(1, "a"), new MyRecord(5, "c"), new MyRecord(10, "c"), new MyRecord(1, "a"), new MyRecord(3, "d"));
        Callable<Object> invoker = compileStream(input, OperatorNode.create(StreamOperator.GROUPBY, accumulate(),
                OperatorNode.create(FunctionOperator.FUNCTION, ImmutableList.of("$row"), OperatorNode.create(PhysicalExprOperator.PROPREF, OperatorNode.create(PhysicalExprOperator.LOCAL, "$row"), "sval")),
                OperatorNode.create(FunctionOperator.FUNCTION, ImmutableList.of("$key", "$rows"),
                        OperatorNode.create(PhysicalExprOperator.INVOKE, ExactInvocation.exactInvoke(Opcodes.INVOKESTATIC, "aggregate",
                                scope.adapt(StreamOperatorCompilerTest.class, false),
                                scope.adapt(MyRecord.class, false),
                                BaseTypeAdapter.STRING,
                                scope.adapt(List.class, false)), ImmutableList.of(OperatorNode.create(PhysicalExprOperator.LOCAL, "$key"), OperatorNode.create(PhysicalExprOperator.LOCAL, "$rows")))
                )
        ));
        Assert.assertEquals(ImmutableList.of(new MyRecord(2, "a"), new MyRecord(15, "c"), new MyRecord(3, "d")), invoker.call());
    }


    @Test
    public void requireCross() throws Exception {
        List<String> left = ImmutableList.of("a", "b");
        List<Integer> right = ImmutableList.of(1, 2);
        List<MyRecord> output = ImmutableList.of(new MyRecord(1, "a"), new MyRecord(2, "a"), new MyRecord(1, "b"), new MyRecord(2, "b"));
        Callable<Object> invoker = compileStream(left, OperatorNode.create(StreamOperator.CROSS, accumulate(),
                constant(right),
                OperatorNode.create(FunctionOperator.FUNCTION, ImmutableList.of("$left", "$right"),
                        OperatorNode.create(PhysicalExprOperator.SINGLETON,
                                OperatorNode.create(PhysicalExprOperator.NEW, scope.adapt(MyRecord.class, false), ImmutableList.of(OperatorNode.create(PhysicalExprOperator.LOCAL, "$right"), OperatorNode.create(PhysicalExprOperator.LOCAL, "$left")))))));
        Assert.assertEquals(output, invoker.call());
    }

    public static class Photo {
        public final int id;
        public final String name;

        public Photo(int id, String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Photo photo = (Photo) o;

            if (id != photo.id) return false;
            return name.equals(photo.name);
        }

        @Override
        public int hashCode() {
            int result = id;
            result = 31 * result + name.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "Photo{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    '}';
        }
    }

    public static class Image {
        public final int id;
        public final int photo_id;
        public final int width;
        public final int height;
        public final String name;

        public Image(int id, int photo_id, int width, int height, String name) {
            this.id = id;
            this.photo_id = photo_id;
            this.width = width;
            this.height = height;
            this.name = name;
        }

        @Override
        public String toString() {
            return "Image{" +
                    "id=" + id +
                    ", photo_id=" + photo_id +
                    ", width=" + width +
                    ", height=" + height +
                    ", name='" + name + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Image image = (Image) o;

            if (height != image.height) return false;
            if (id != image.id) return false;
            if (photo_id != image.photo_id) return false;
            if (width != image.width) return false;
            return name != null ? name.equals(image.name) : image.name == null;
        }

        @Override
        public int hashCode() {
            int result = id;
            result = 31 * result + photo_id;
            result = 31 * result + width;
            result = 31 * result + height;
            result = 31 * result + (name != null ? name.hashCode() : 0);
            return result;
        }
    }

    // test & prototype some of the strategies used to implement JOIN

    public static List<Image> lookupImage(int photo_id) {
        int base_id = photo_id * 100;
        return ImmutableList.of(
                new Image(base_id, photo_id, 300, 200, "small"),
                new Image(base_id + 1, photo_id, 600, 400, "medium"),
                new Image(base_id + 2, photo_id, 1200, 800, "large"));
    }

    // two scenarios
    //  1) we have a single lookup method (as above) -- in this case, we want to do the scattering lookup in parallel
    //  2) we have a batch lookup method -- in this case, we need to do a single call into the source, so we call it once, then we need
    //     to make a lookup table so we can do the join.
    //  (we could also just do the join as a cross join with a predicate)

    private OperatorNode<PhysicalExprOperator> doSingleLookupImage(OperatorNode<PhysicalExprOperator> key) {
        return OperatorNode.create(PhysicalExprOperator.INVOKE, ExactInvocation.exactInvoke(Opcodes.INVOKESTATIC, "lookupImage",
                        scope.adapt(StreamOperatorCompilerTest.class, false),
                        new ListTypeWidget(scope.adapt(Image.class, false)),
                        BaseTypeAdapter.INT32),
                ImmutableList.of(key));
    }

    // prototype/test the first scenario
    @Test
    public void requireScatterJoin() throws Exception {
        List<Photo> input = ImmutableList.of(new Photo(1, "bob"), new Photo(2, "joe"), new Photo(3, "smith"), new Photo(1, "bob twice"));
        // we want JOIN Photo to Image
        // output records of photo, image

        // we're going to finish with an accumulate
        OperatorNode<StreamOperator> stream = accumulate();

        // to simplify the test, transform from the record output into just the image side
        stream = OperatorNode.create(StreamOperator.TRANSFORM, stream,
                OperatorNode.create(FunctionOperator.FUNCTION, ImmutableList.of("$row"),
                        OperatorNode.create(PhysicalExprOperator.PROPREF, OperatorNode.create(PhysicalExprOperator.LOCAL, "$row"), "image")));

        // we need to flatten the output of the scatter operation
        stream = OperatorNode.create(StreamOperator.FLATTEN, stream);

        // scatter based on the output of the groupby
        stream = OperatorNode.create(StreamOperator.TRANSFORM, stream,
                OperatorNode.create(FunctionOperator.FUNCTION, ImmutableList.of("$row"),
                        OperatorNode.create(PhysicalExprOperator.STREAM_EXECUTE,
                                OperatorNode.create(PhysicalExprOperator.PROPREF, OperatorNode.create(PhysicalExprOperator.LOCAL, "$row"), "left_rows"),
                                OperatorNode.create(StreamOperator.CROSS,
                                        accumulate(),
                                        doSingleLookupImage(OperatorNode.create(PhysicalExprOperator.PROPREF, OperatorNode.create(PhysicalExprOperator.LOCAL, "$row"), "key")),
                                        OperatorNode.create(FunctionOperator.FUNCTION, ImmutableList.of("$left", "$right"),
                                                OperatorNode.create(PhysicalExprOperator.SINGLETON,
                                                        OperatorNode.create(PhysicalExprOperator.RECORD,
                                                                ImmutableList.of("photo", "image"),
                                                                ImmutableList.of(
                                                                        OperatorNode.create(PhysicalExprOperator.LOCAL, "$left"),
                                                                        OperatorNode.create(PhysicalExprOperator.LOCAL, "$right")))))))));

        // groupby the input key so we only call the source once per input key
        stream = OperatorNode.create(StreamOperator.GROUPBY, stream,
                OperatorNode.create(FunctionOperator.FUNCTION, ImmutableList.of("$row"), OperatorNode.create(PhysicalExprOperator.PROPREF, OperatorNode.create(PhysicalExprOperator.LOCAL, "$row"), "id")),
                OperatorNode.create(FunctionOperator.FUNCTION, ImmutableList.of("$key", "$rows"),
                        OperatorNode.create(PhysicalExprOperator.RECORD,
                                ImmutableList.of("key", "left_rows"),
                                ImmutableList.of(
                                        OperatorNode.create(PhysicalExprOperator.LOCAL, "$key"),
                                        OperatorNode.create(PhysicalExprOperator.LOCAL, "$rows")))));


        Callable<Object> invoker = compileStream(input, stream);

        Assert.assertEquals(invoker.call(),
                // bob's photos are duplicated because bob's id shows up twice in the input
                ImmutableList.copyOf(Iterables.concat(lookupImage(1), lookupImage(1), lookupImage(2), lookupImage(3))));
    }

    @Test
    public void requireHashJoin() throws Exception {
        List<Photo> photos = ImmutableList.of(new Photo(1, "bob"), new Photo(2, "joe"), new Photo(3, "smith"), new Photo(1, "bob twice"));
        List<Image> images = ImmutableList.copyOf(Iterables.concat(lookupImage(1), lookupImage(2), lookupImage(3)));

        // we're going to finish with an accumulate
        OperatorNode<StreamOperator> stream = accumulate();

        // to simplify the test, transform from the record output into just the image side
        stream = OperatorNode.create(StreamOperator.TRANSFORM, stream,
                OperatorNode.create(FunctionOperator.FUNCTION, ImmutableList.of("$row"),
                        OperatorNode.create(PhysicalExprOperator.PROPREF, OperatorNode.create(PhysicalExprOperator.LOCAL, "$row"), "image")));

        // do a hash join
        // HASH_JOIN(right_sequence, (left) -> key, (right) -> key, (left, right) -> row, (left) -> row_or_null)

        stream = OperatorNode.create(StreamOperator.HASH_JOIN,
                stream,
                constant(images),
                OperatorNode.create(FunctionOperator.FUNCTION,
                        ImmutableList.of("$row"),
                        OperatorNode.create(PhysicalExprOperator.PROPREF,
                                OperatorNode.create(PhysicalExprOperator.LOCAL, "$row"), "id")),
                OperatorNode.create(FunctionOperator.FUNCTION,
                        ImmutableList.of("$row"),
                        OperatorNode.create(PhysicalExprOperator.PROPREF,
                            OperatorNode.create(PhysicalExprOperator.LOCAL, "$row"), "photo_id")),
                OperatorNode.create(FunctionOperator.FUNCTION, ImmutableList.of("$left", "$right"),
                            OperatorNode.create(PhysicalExprOperator.RECORD,
                                    ImmutableList.of("photo", "image"),
                                    ImmutableList.of(
                                            OperatorNode.create(PhysicalExprOperator.LOCAL, "$left"),
                                            OperatorNode.create(PhysicalExprOperator.LOCAL, "$right")))));

        Callable<Object> invoker = compileStream(photos, stream);

        Assert.assertEquals(invoker.call(),
                // bob's photos are duplicated because bob's id shows up twice in the input
                ImmutableList.copyOf(Iterables.concat(lookupImage(1), lookupImage(2), lookupImage(3), lookupImage(1))));
    }

    @Test
    public void requireOuterHashJoin() throws Exception {
        List<Photo> photos = ImmutableList.of(new Photo(1, "bob"), new Photo(3, "joe"));
        List<Image> images = ImmutableList.copyOf(lookupImage(1));

        // we're going to finish with an accumulate
        OperatorNode<StreamOperator> stream = accumulate();

        // to simplify the test, transform from the record output into just the photo side
        stream = OperatorNode.create(StreamOperator.TRANSFORM, stream,
                OperatorNode.create(FunctionOperator.FUNCTION, ImmutableList.of("$row"),
                        OperatorNode.create(PhysicalExprOperator.PROPREF, OperatorNode.create(PhysicalExprOperator.LOCAL, "$row"), "photo")));

        // do a hash join
        // HASH_JOIN(right_sequence, (left) -> key, (right) -> key, (left, right) -> row, (left) -> row_or_null)

        stream = OperatorNode.create(StreamOperator.OUTER_HASH_JOIN,
                stream,
                constant(images),
                OperatorNode.create(FunctionOperator.FUNCTION,
                        ImmutableList.of("$row"),
                        OperatorNode.create(PhysicalExprOperator.PROPREF,
                                OperatorNode.create(PhysicalExprOperator.LOCAL, "$row"), "id")),
                OperatorNode.create(FunctionOperator.FUNCTION,
                        ImmutableList.of("$row"),
                        OperatorNode.create(PhysicalExprOperator.PROPREF,
                                OperatorNode.create(PhysicalExprOperator.LOCAL, "$row"), "photo_id")),
                OperatorNode.create(FunctionOperator.FUNCTION, ImmutableList.of("$left", "$right"),
                        OperatorNode.create(PhysicalExprOperator.RECORD,
                                ImmutableList.of("photo", "image"),
                                ImmutableList.of(
                                        OperatorNode.create(PhysicalExprOperator.LOCAL, "$left"),
                                        OperatorNode.create(PhysicalExprOperator.LOCAL, "$right")))));

        Callable<Object> invoker = compileStream(photos, stream);

        Assert.assertEquals(invoker.call(),
                // bob's photos are duplicated because bob's id shows up twice in the input
                ImmutableList.of(new Photo(1, "bob"), new Photo(1, "bob"), new Photo(1, "bob"), new Photo(3, "joe")));
    }
    private OperatorNode<StreamOperator> accumulate() {
        return OperatorNode.create(StreamOperator.SINK, OperatorNode.create(SinkOperator.ACCUMULATE));
    }

}
