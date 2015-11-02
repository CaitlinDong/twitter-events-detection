package ted

import edu.stanford.nlp.ie.AbstractSequenceClassifier
import edu.stanford.nlp.ie.crf._
import edu.stanford.nlp.io.IOUtils
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.sequences.DocumentReaderAndWriter
import edu.stanford.nlp.util.Triple
import java.util.List

/**
 * Created by namitsharma on 11/2/15.
 */
object NER {
  def main(args: Array[String]): Unit = {
    var serializedClassifier: String = "src/main/resources/classifiers/english.all.3class.distsim.crf.ser.gz"
    val classifier: AbstractSequenceClassifier[CoreLabel] = CRFClassifier.getClassifier(serializedClassifier)
    val example: Array[String] = Array("Good afternoon Rajat Raina, how are you today?", "I go to school at Stanford University, which is located in California.")
    for (str <- example) {
      System.out.println(classifier.classifyWithInlineXML(str))
    }
    System.out.println("---")
  }
}
