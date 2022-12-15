package org.example.utils;

import org.apache.log4j.Level;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import java.util.ArrayList;
import java.util.List;

//spark配置選項
public class DriverCLIOptions {

	@Option(name = "-h", aliases = { "--host" }, usage = "bind driver program to host adress ('spark.driver.host'). default: will try to guess", required = false)
	private String driverHost = null;

	@Option(name = "-p", aliases = { "--port" }, usage = "bind driver program to specified port ('spark.driver.port'). default: random", required = false)
	private String driverPort = null;

	@Option(name = "-n", aliases = { "--name" }, usage = "set driver program name. default: 'Refine-Spark CLI'", required = false)
	private String appName = "Refine-Spark CLI";
	@Option(name = "-v", aliases = { "--log" }, usage = "set verbosity level to INFO. default: 'WARN'", required = false)
	public boolean verbosity = false;

	@Option(name = "-m", usage = "set executor memory size default: 1g", required = false)
	String executorMemory = "1g";

	@Option(name = "-f", aliases = { "--fsblock" }, usage = "set fs.local.block.size in mb, default: 32 mb", required = false)
	Long fsBlockSize = 32L;

	@Argument
	private List<String> fArguments = new ArrayList<String>();

	public void setDriverHost(String driverHost) {
		this.driverHost = driverHost;
	}

	public void setDriverPort(String driverPort) {
		this.driverPort = driverPort;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	public void setVerbosity(boolean verbosity) {
		this.verbosity = verbosity;
	}

	public void setExecutorMemory(String executorMemory) {
		this.executorMemory = executorMemory;
	}

	public void setFsBlockSize(Long fsBlockSize) {
		this.fsBlockSize = fsBlockSize;
	}

	public void setfArguments(List<String> fArguments) {
		this.fArguments = fArguments;
	}

	public String getDriverHost() {
		return driverHost;
	}

	public String getDriverPort() {
		return driverPort;
	}

	public String getAppName() {
		return appName;
	}

	public Level getVerbose() {
		return (verbosity) ? Level.INFO : Level.WARN;
	}

	public String getExecutorMemory() {
		return executorMemory;
	}

	public Long getFsBlockSize() {
		return fsBlockSize * 1024 * 1024;
	}

	public List<String> getArguments() {
		return fArguments;
	}

}