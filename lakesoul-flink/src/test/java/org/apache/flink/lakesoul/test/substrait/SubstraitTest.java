package org.apache.flink.lakesoul.test.substrait;

import com.dmetasoul.lakesoul.lakesoul.io.NativeIOReader;
import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.*;
import io.substrait.expression.Expression;
import io.substrait.expression.proto.ExpressionProtoConverter;
import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.SimpleExtension;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.relation.*;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import org.apache.flink.lakesoul.substrait.SubstraitUtil;
import org.apache.flink.table.expressions.*;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.IntType;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SubstraitTest {
    @Test
    public void generalExprTest() throws IOException {
        ValueLiteralExpression valExpr = new ValueLiteralExpression(3, new AtomicDataType(new IntType(false)));
        FieldReferenceExpression orderId = new FieldReferenceExpression("order_id",
                new AtomicDataType(new IntType())
                , 0, 0);
        List<ResolvedExpression> args = new ArrayList<>();
        args.add(orderId);
        args.add(valExpr);
        // naive binary func : gt gte lt lte
        CallExpression eq = funcInvoke(BuiltInFunctionDefinitions.EQUALS, args);
        CallExpression lt = funcInvoke(BuiltInFunctionDefinitions.LESS_THAN, args);
        CallExpression lte = funcInvoke(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, args);
        CallExpression gt = funcInvoke(BuiltInFunctionDefinitions.GREATER_THAN, args);
        CallExpression gte = funcInvoke(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, args);
        CallExpression and = funcInvoke(BuiltInFunctionDefinitions.AND, args);
        CallExpression or = funcInvoke(BuiltInFunctionDefinitions.OR, args);
        // naive unary func : is null , is not null
        args.clear();
        args.add(orderId);
        CallExpression nonNull = funcInvoke(BuiltInFunctionDefinitions.IS_NOT_NULL, args);
        CallExpression isNull = funcInvoke(BuiltInFunctionDefinitions.IS_NULL, args);
        // compound  expr
        args.clear();
        args.add(lt);
        args.add(lte);
        funcInvoke(BuiltInFunctionDefinitions.EQUALS, args);
        args.clear();
        args.add(isNull);
        funcInvoke(BuiltInFunctionDefinitions.NOT,args);
    }

    CallExpression funcInvoke(BuiltInFunctionDefinition func, List<ResolvedExpression> args) {
        CallExpression expr = CallExpression.permanent(func, args, new AtomicDataType(new BooleanType()));
        Expression e = SubstraitUtil.doTransform(expr);
        System.out.println(e);
        return expr;
    }

    @Test
    public void literalExprTest() {
        ValueLiteralExpression valExpr = new ValueLiteralExpression(3, new AtomicDataType(new IntType(false)));
        Expression substraitExpr = SubstraitUtil.doTransform(valExpr);
        System.out.println(substraitExpr);
    }

    @Test
    public void FieldRefTest() {
        FieldReferenceExpression orderId = new FieldReferenceExpression("order_id",
                new AtomicDataType(new IntType())
                , 0, 0);
        Expression expr = SubstraitUtil.doTransform(orderId);
        System.out.println(expr);
        System.out.println(toProto(null, expr));
    }

    private io.substrait.proto.Expression toProto(ExtensionCollector collector, Expression expr) {
        return expr.accept(new ExpressionProtoConverter(collector, null));
    }

    @Test
    public void callExprTest() {
        try {
            SimpleExtension.ExtensionCollection extensionCollection = SimpleExtension.loadDefaults();
            System.out.println(extensionCollection.scalarFunctions());
            SimpleExtension.ScalarFunctionVariant desc = extensionCollection.getScalarFunction(SimpleExtension.FunctionAnchor.of("/functions_comparison.yaml", "equal:any_any"));
            Expression.ScalarFunctionInvocation si = ExpressionCreator.scalarFunction(desc, TypeCreator.NULLABLE.I32);
            io.substrait.proto.Expression p = toProto(new ExtensionCollector(), si);
            System.out.println(p);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void endToEndTest() {
        try {
            SimpleExtension.ExtensionCollection se = SimpleExtension.loadDefaults();
            SubstraitBuilder b = new SubstraitBuilder(se);
            List<String> tableName = Stream.of("a_table").collect(Collectors.toList());
            List<String> columnNames = Stream.of("col1", "col2").collect(Collectors.toList());
            TypeCreator R = TypeCreator.REQUIRED;
            List<Type> columnTypes = Stream.of(R.I32, R.I32).collect(Collectors.toList());
            NamedScan namedScan = b.namedScan(tableName, columnNames, columnTypes);
            namedScan =
                    NamedScan.builder()
                            .from(namedScan)
                            .filter(b.equal(b.fieldReference(namedScan, 1), b.i32(3)))
                            .build();


            Plan.Root root = b.root(namedScan);
            Plan plan = b.plan(root);
            System.out.println(plan);
            PlanProtoConverter planProtoConverter = new PlanProtoConverter();
            io.substrait.proto.Plan proto = planProtoConverter.toProto(plan);
            System.out.println(proto);
//            byte[] byteArray = proto.toByteArray();
//            System.out.println(Arrays.toString(byteArray));
            NativeIOReader reader = new NativeIOReader();
//            reader.addFilterProto(proto);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
