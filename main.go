package main

import (
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"encoding/json"
	"JSONDumpMongo/filehelper"
	model"JSONDumpMongo/models"
	"context"
	"io"
	"io/ioutil"
	"os"
	u "JSONDumpMongo/logger"
	"time"
	"github.com/manifoldco/promptui"
	"fmt"
	"github.com/cheggaaa/pb/v3"
)

const (
	banner = `
	_______  _______  _        _______  _______ _________ _        _______  _______  _______ _________
	(       )(  ___  )( (    /|(  ____ \(  ___  )\__   __/( (    /|(  ____ \(  ____ \(  ____ )\__   __/
	| () () || (   ) ||  \  ( || (    \/| (   ) |   ) (   |  \  ( || (    \/| (    \/| (    )|   ) (   
	| || || || |   | ||   \ | || |      | |   | |   | |   |   \ | || (_____ | (__    | (____)|   | |   
	| |(_)| || |   | || (\ \) || | ____ | |   | |   | |   | (\ \) |(_____  )|  __)   |     __)   | |   
	| |   | || |   | || | \   || | \_  )| |   | |   | |   | | \   |      ) || (      | (\ (      | |   
	| )   ( || (___) || )  \  || (___) || (___) |___) (___| )  \  |/\____) || (____/\| ) \ \__   | |   
	|/     \|(_______)|/    )_)(_______)(_______)\_______/|/    )_)\_______)(_______/|/   \__/   )_(   `
	
)

func main(){
	

	// Print Banner //
	fmt.Println(banner)


	//Option Selection //
	prompt := promptui.Select{
		Label: "Select Day",
		Items: []string{"Start", "Stop"},
	}

	_, result, err := prompt.Run()

	if err != nil {
		fmt.Printf("Prompt failed %v\n", err)
		return
	}

	fmt.Printf("You choose %q\n", result)

	//If User Selects "Start"
	if result == "Start"{
		//Mark Start Time
		start := time.Now()
		u.GeneralLogger.Println("Starting Utility..",start)
		
		//get config data
		configs,err := GetConfigData()
		
		//if configuration is valid start utitlity
		if err == nil {
			ctx := context.Background()
			
			client, err := mongo.Connect(ctx, options.Client().ApplyURI(configs.DatabaseURL))
			
			if err != nil {
				u.ErrorLogger.Println("An Error Occured to Open Database",err)
				panic(err)
			}

			db := client.Database(configs.DatabaseName)
			defer db.Client().Disconnect(ctx)
			
			col := db.Collection(configs.CollectionURL)

			// transaction
			err = db.Client().UseSession(ctx, func(sessionContext mongo.SessionContext) error {
				err := sessionContext.StartTransaction()
				if err != nil {
					u.ErrorLogger.Println("An Error Occured: Mongo Transaction",err)
					return err
				}

				//check if directory is empty or not  present  then stop the utility
				isDirEmpty,err:=IsDirEmpty(configs.InputFolderPath)
				if isDirEmpty{
					u.ErrorLogger.Println("An Error Occured: Directory is Empty",err)
					fmt.Println("Invalid Directory or Empty")
					os.Exit(1)
				}
				
				files, err := ioutil.ReadDir(configs.InputFolderPath)
				if err != nil {
					u.ErrorLogger.Println("An Error Occured: ReadDir()",err)
				}
				fmt.Println("Total Files Found",len(files))
				u.GeneralLogger.Println("Total File Found..",len(files))
				
				count := len(files)
				// create and start new bar
				bar := pb.StartNew(count)

				for _, f := range files {
					var documentInsertion error

					interfaceData:=model.JSONFileData{}
					data,err := filehelper.ReadFile(configs.InputFolderPath+f.Name())
					if err!=nil{
						u.ErrorLogger.Println("An Error Occured: While Opening JSON File:",f.Name(),":",err)
					}
					
					documentInsertion = json.Unmarshal(data, &interfaceData.Data)
					if documentInsertion != nil {
						u.ErrorLogger.Println("An Error Occured: Unmarshling Failed:",f.Name(),":",err)
						os.Rename(configs.InputFolderPath+f.Name(), configs.ErrorFolderPath+f.Name())
					}
					
					if documentInsertion == nil{
					
						_, err = col.InsertOne(sessionContext, interfaceData.Data)
						if err != nil {
							u.ErrorLogger.Println("An Error Occured: Document Insertion Failed:",f.Name(),":",err)

						}else{
							err := os.Rename(configs.InputFolderPath+f.Name(), configs.OutputFolderPath+f.Name())
							if err != nil {
								u.GeneralLogger.Println(" Moving File to Output Folder:",f.Name(),":",err)
							}
						}
					}else{
						u.ErrorLogger.Println("An Error Occured: SKIPPING INSERTION:",f.Name(),":",err)
					}
					bar.Increment()
					time.Sleep(time.Millisecond)

				}
				
				
				if err != nil {
					sessionContext.AbortTransaction(sessionContext)
					u.ErrorLogger.Println("An Error Occured: Transaction Aborted:",err)
					return err
				} else{
					sessionContext.CommitTransaction(sessionContext)
					u.GeneralLogger.Println("Done !")
					bar.Finish()
				}
				return nil
			})
			elapsed := time.Since(start)
			u.GeneralLogger.Println("Time took to insert all documents..",elapsed)
		}else{
			os.Exit(1)
			u.ErrorLogger.Println("An Error Occured: Aborted:",err)
		}
	
	}else if (result == "Stop"){
		os.Exit(1)
		fmt.Println("Stopped by User")
	}else{
		fmt.Printf("Inavlid Input %q\n", result)
	}




}

func GetConfigData() (model.ConfigDetails,error) {
	ConfigData:= model.ConfigDetails{}
	configDataPath := "./config/config.json"

	data,err:=filehelper.ReadFile(configDataPath)
	if err!=nil{
		u.ErrorLogger.Println("An Error Occured: Config File Read:",err)
		return model.ConfigDetails{},err
	}
	
	err = json.Unmarshal(data, &ConfigData)
    if err != nil {
		u.ErrorLogger.Println("An Error Occured: Config File Unmarshal:",err)
		return model.ConfigDetails{},err
    }
	return ConfigData,nil

}
func IsDirEmpty(name string) (bool, error) {
	f, err := os.Open(name)
	if err != nil {
			return false, err
	}
	defer f.Close()

	// read in ONLY one file
	_, err = f.Readdir(1)

	// and if the file is EOF... well, the dir is empty.
	if err == io.EOF {
			return true, nil
	}
	return false, err
}

