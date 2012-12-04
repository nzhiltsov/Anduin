package ru.ksu.niimm.cll.anduin;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.TemplateTap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import scala.Tuple3;

import java.io.IOException;
import java.util.Properties;

/**
 * This job aggregates entity info according to entity URI and works only in the Hadoop mode
 *
 * @author Nikita Zhiltsov
 */
public class DocumentEntitySplitter {
    private static String inputPath;
    private static String outputPath;
    public static final String INPUT_ARG_NAME = "input";
    public static final String OUTPUT_ARG_NAME = "output";

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, DocumentEntitySplitter.class);
        if (!parseArguments(args)) {
            return;
        }
        TextLine sourceScheme = new TextLine(new Fields("line"));
        Tap source = new Hfs(sourceScheme, inputPath);
        TextDelimited sinkScheme = new TextDelimited(new Fields("predicate", "object"), " ");
        Hfs parentOutput = new Hfs(sinkScheme, outputPath);
        TemplateTap childrenOutput = new TemplateTap(parentOutput, "%s", new Fields("hash", "predicate", "object"),
                SinkMode.REPLACE);
        Pipe assembly = new Pipe("DE-splitter");
        assembly = new Each(assembly, new Fields("line"), new AddNodesFunction());

        FlowConnector flowConnector = new HadoopFlowConnector(properties);
        Flow flow = flowConnector.connect("DE-splitter", source, childrenOutput, assembly);
        flow.start();
        flow.complete();
    }

    public static final class AddNodesFunction extends BaseOperation implements Function {
        public AddNodesFunction() {
            super(1, new Fields("hash", "predicate", "object"));
        }

        public AddNodesFunction(Fields fieldDeclaration) {
            super(1, fieldDeclaration);
        }

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            Tuple3<String, String, String> nodes =
                    NodeParser$.MODULE$.extractNodesFromNTuple(functionCall.getArguments().getString(0));
            Tuple result = new Tuple();
            result.add(nodes._1().hashCode());
            result.add(nodes._2());
            result.add(nodes._3());
            functionCall.getOutputCollector().add(result);
        }
    }

    private static boolean parseArguments(String[] args) throws IOException {
        OptionParser optionParser = new OptionParser();
        optionParser.accepts(INPUT_ARG_NAME, "The inpute file path.")
                .withRequiredArg().ofType(String.class);
        optionParser.accepts(OUTPUT_ARG_NAME,
                "The parent directory for output sub-folders.").withRequiredArg().ofType(String.class);
        OptionSet optionSet = optionParser.parse(args);
        if (optionSet.has(INPUT_ARG_NAME) && optionSet.has(OUTPUT_ARG_NAME)) {
            inputPath = (String) optionSet.valueOf(INPUT_ARG_NAME);
            outputPath = (String) optionSet.valueOf(OUTPUT_ARG_NAME);
            if (inputPath == null || outputPath == null) {
                optionParser.printHelpOn(System.out);
                return false;
            }
            System.out.println("INPUT PATH: " + inputPath + " " + "OUTPUT PATH: " + outputPath);
            return true;
        } else {
            optionParser.printHelpOn(System.out);
            return false;
        }
    }
}
