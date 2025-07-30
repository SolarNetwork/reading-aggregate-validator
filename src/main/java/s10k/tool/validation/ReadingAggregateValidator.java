package s10k.tool.validation;

import java.util.concurrent.Callable;

import org.springframework.stereotype.Component;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Validate SolarNetwork aggregate reading values.
 */
@Component
@Command(name = "validate")
public class ReadingAggregateValidator implements Callable<Integer> {

	@Option(names = { "-n", "--dry-run" }, description = "do not actually submit changes to SolarNetwork")
	boolean dryRun;

	@Option(names = { "-j", "--threads" }, description = "number of concurrent threads")
	int threadCount = 4;

	@Option(names = { "-h", "--help" }, usageHelp = true, description = "display this help message")
	boolean usageHelpRequested;

	@Override
	public Integer call() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
