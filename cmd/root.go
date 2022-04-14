package cmd

import (
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/spf13/cobra"
)

var (
	// Default values may be set at compile time.
	version          = "0.1.4"
	buildDate        = "2020-01-02T03:04+0500"
	osArch           = "darwin"
	stackDumpOnPanic bool
	silenceUsage     bool
)

var rootCmd = &cobra.Command{
	Use: "hp",
	// Short: "HalfPipe is a small but powerful utility to easily stream data.",
	Long: ` 
  ___ ___        .__   _____        __________.__               
 /   |   \_____  |  |_/ ____\       \______   \__|_____   ____  
/    ~    \__  \ |  |\   __\  ______ |     ___/  \____ \_/ __ \ 
\    Y    // __ \|  |_|  |   /_____/ |    |   |  |  |_> >  ___/ 
 \___|_  /(____  /____/__|           |____|   |__|   __/ \___  >
       \/      \/                                |__|        \/ 

Halfpipe is a DataOps utility for streaming data. It's designed to be light-weight and easy to use.
Use command-line switches for pre-canned actions or write your own pipes in YAML or JSON to sync 
data in near real-time. Start an HTTP server to expose functionality via a RESTful API.
Halfpipe is not yet cluster-aware but it scales out. Start multiple instances of this tool and
off you go. Happy munging! 😄`,
}

func init() {
	// General setup.
	cobra.EnableCommandSorting = false
	// Global flags.
	rootCmd.PersistentFlags().BoolVar(&stackDumpOnPanic, "print-stack", false, "Print a stack dump if there is a panic")
	_ = rootCmd.PersistentFlags().MarkHidden("print-stack")
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if twelveFactorMode { // if we are running based on environment variables...
		if lambdaMode { // if we should handle lambda execution...
			lambda.Start(func() error { return execute12FactorMode(twelveFactorActions) })
		} else {
			if err := execute12FactorMode(twelveFactorActions); err != nil {
				// execute12FactorMode prints the error.
				os.Exit(1)
			}
		}
	} else { // else we're using CLI args and flags via Cobra...
		if err := rootCmd.Execute(); err != nil {
			// Execute() prints the error.
			os.Exit(1)
		}
	}
}

// Commented setup of global config since it is not used yet.
// INSTEAD use package config.Main.
// initConfig reads in config file and ENV variables if set.
// func initConfig() {
// 	if cfgFile != "" { // if the user overrides the config file...
// 		// Use config file from the flag.
// 		viper.SetConfigFile(cfgFile)
// 	} else { // else we are using the default config path...
// 		viper.SetConfigFile(config.Main.FullPath)
// 	}
// }

// func junk() {
// 	viper.AddConfigPath(config.mustGetConfigHomeDir())
// 	viper.SetConfigName(config.MainFileNamePrefix) // no extension.
// 	configFileName := path.Join(config.mustGetConfigHomeDir(), config.MainFileFullName)
// 	// Test read the config.
// 	viper.AutomaticEnv() // read in environment variables that match.
// 	if err := viper.ReadInConfig(); err != nil { // if it doesn't exist...
// 		// Create default config file.
// 		err := config.makeDir(config.mustGetConfigHomeDir())
// 		if err != nil {
// 			fmt.Println(err)
// 			os.Exit(1)
// 		}
// 		if err := viper.WriteConfigAs(configFileName); err == nil {
// 			fmt.Printf("Default config file created: %v\n", configFileName)
// 		} else {
// 			fmt.Println(err)
// 			os.Exit(1)
// 		}
// 	}
// }

// Richard 20190812, keeping this OnInitialise() as a reminder of capability:
// cobra.OnInitialize(initConfig)
// Richard, comment config file flags while we're in WIP mode:
// rootCmd.PersistentFlags().StringVar(&cfgFile, "config-file", "",fmt.Sprintf("Config `<file>` (default: %v)", config.Main.FullPath))
// rootCmd.PersistentFlags().StringVar(&cfgFile, "connections-file", "",fmt.Sprintf("Connections `<file>` (default: %v)", config.Connections.FullPath))
// _ = rootCmd.MarkPersistentFlagFilename("config-file", "properties")
// _ = rootCmd.MarkPersistentFlagFilename("connections-file", "yaml")

// Binding variables junk:
// viperSF := viper.New()
// viperSF.AutomaticEnv()
// _ = viperSF.BindPFlag("HP_SQL_DATE_FIELD", f.Lookup("sql-date-field-name"))
