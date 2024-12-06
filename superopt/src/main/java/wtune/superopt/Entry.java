package wtune.superopt;

import wtune.common.utils.IOSupport;
import wtune.superopt.fragment.Fragment;
import wtune.superopt.logic.LogicSupport;
import wtune.superopt.runner.Runner;
import wtune.superopt.substitution.Substitution;
import wtune.superopt.substitution.SubstitutionBank;
import wtune.superopt.substitution.SubstitutionSupport;
import wtune.superopt.uexpr.UExprSupport;
import wtune.superopt.uexpr.UExprTranslationResult;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.logging.LogManager;

import static wtune.common.io.FileUtils.dataDir;

public class Entry {
  private static final String LOGGER_CONFIG =
      ".level = INFO\n"
          + "java.util.logging.ConsoleHandler.level = INFO\n"
          + "handlers=java.util.logging.ConsoleHandler\n"
          + "java.util.logging.ConsoleHandler.formatter=java.util.logging.SimpleFormatter\n"
          + "java.util.logging.SimpleFormatter.format=[%1$tm/%1$td %1$tT][%3$10s][%4$s] %5$s %n\n";

  static {
    try {
      LogManager.getLogManager()
          .readConfiguration(new ByteArrayInputStream(LOGGER_CONFIG.getBytes()));
    } catch (IOException ignored) {
    }
  }

  public static void main(String[] args) throws Exception {
    if (!args[0].startsWith("runner.")) args[0] = "runner." + args[0];
    final String clsName = Entry.class.getPackageName() + "." + args[0];
    final Class<?> cls = Class.forName(clsName);

    if (!Runner.class.isAssignableFrom(cls)) {
      System.err.println("not a runner");
      return;
    }

    final Runner runner = (Runner) cls.getConstructor().newInstance();
    runner.prepare(args);
    runner.run();
    runner.stop();
//    studyWetuneRules();
  }

  static void studyWetuneRules() throws IOException {
    final Path ruleFilePath = dataDir().resolve("prepared").resolve("rules.txt");
    final SubstitutionBank rules = SubstitutionSupport.loadBank(ruleFilePath);
    final Path dir = dataDir().resolve("prepared").resolve("rules_trees.txt");
    for (Substitution rule : rules.rules()) {
      Fragment fragment0 = rule._0();
      IOSupport.appendTo(dir, out -> out.println(fragment0.toTreeListStr()));
      Fragment fragment1 = rule._1();
      IOSupport.appendTo(dir, out -> out.println(fragment1.toTreeListStr()));
    }
  }
}
