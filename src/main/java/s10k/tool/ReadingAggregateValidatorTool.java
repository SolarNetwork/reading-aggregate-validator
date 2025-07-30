package s10k.tool;

import org.springframework.boot.Banner.Mode;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

import picocli.CommandLine;
import picocli.CommandLine.IFactory;
import s10k.tool.validation.ReadingAggregateValidator;

/**
 * SolarNetwork reading aggregate validation command-line tool.
 */
@SpringBootApplication
public class ReadingAggregateValidatorTool implements CommandLineRunner, ExitCodeGenerator {

	private final IFactory factory;
	private final ReadingAggregateValidator validatorCommand;

	private int exitCode;

	/**
	 * Constructor.
	 * 
	 * @param factory          the command factory
	 * @param validatorCommand the command
	 */
	public ReadingAggregateValidatorTool(IFactory factory, ReadingAggregateValidator validatorCommand) {
		super();
		this.factory = factory;
		this.validatorCommand = validatorCommand;
	}

	@Override
	public void run(String... args) throws Exception {
		exitCode = new CommandLine(validatorCommand, factory).execute(args);
	}

	@Override
	public int getExitCode() {
		return exitCode;
	}

	/**
	 * Main entry point.
	 * 
	 * @param args the arguments
	 */
	public static final void main(String[] args) {
		System.exit(SpringApplication.exit(new SpringApplicationBuilder().sources(ReadingAggregateValidatorTool.class)
				.web(WebApplicationType.NONE).logStartupInfo(false).bannerMode(Mode.OFF).build().run(args)));
	}

}