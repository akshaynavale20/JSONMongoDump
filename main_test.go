package main 
 
import (
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"encoding/json"
	"JSONDumpMongo/filehelper"
	model"JSONDumpMongo/models"
	"context"

	"io/ioutil"
	"os"
	u "JSONDumpMongo/logger"
	"time"

	"fmt"
	"reflect"
	"testing"
)
 
func TestMain(t *testing.T) {

	test_json :=model.ConfigDetails{
		DatabaseURL: "mongodb+srv://akshay:akshay@cluster0-dh23q.mongodb.net/test?retryWrites=true&w=majority",
		DatabaseName:"test",
		CollectionURL : "test",
		InputFolderPath: "C:/Akshay/",
		OutputFolderPath: "C:/Output/",
		ErrorFolderPath:"C:/ERROR",
	};

	configs,err:=GetConfigData()

	if  reflect.DeepEqual(test_json, configs){
		t.Errorf("GetConfig() error %v",configs)
	}
	//if configuration is valid start utitlity
	if err == nil {
		ctx := context.Background()
		
		client, err := mongo.Connect(ctx, options.Client().ApplyURI(configs.DatabaseURL))
		
		if err != nil {
			t.Errorf("main() error %v",err)
			panic(err)
		}

		db := client.Database(configs.DatabaseName)
		defer db.Client().Disconnect(ctx)
		
		col := db.Collection(configs.CollectionURL)

		// transaction
		err = db.Client().UseSession(ctx, func(sessionContext mongo.SessionContext) error {
			err := sessionContext.StartTransaction()
			if err != nil {
				t.Errorf("main() error %v",err)
				return err
			}

			//check if directory is empty or not  present  then stop the utility
			isDirEmpty,err:=IsDirEmpty(configs.InputFolderPath)
			if isDirEmpty{
				t.Errorf("main() error %v",err)
				fmt.Println("Invalid Directory or Empty")
			
				os.Exit(1)
			}
			
			files, err := ioutil.ReadDir(configs.InputFolderPath)
			if err != nil {
				t.Errorf("main() error %v",err)
			}
			
			
		

			for _, f := range files {
				var documentInsertion error

				interfaceData:=model.JSONFileData{}
				data,err := filehelper.ReadFile(configs.InputFolderPath+f.Name())
				if err!=nil{
					u.ErrorLogger.Println("An Error Occured: While Opening JSON File:",f.Name(),":",err)
					t.Errorf("main() error %v",err)
				}
				
				documentInsertion = json.Unmarshal(data, &interfaceData.Data)
				if documentInsertion != nil {
					t.Errorf("main() error %v",err)
					u.ErrorLogger.Println("An Error Occured: Unmarshling Failed:",f.Name(),":",err)
					os.Rename(configs.InputFolderPath+f.Name(), configs.ErrorFolderPath+f.Name())
				}
				
				if documentInsertion == nil{
				
					_, err = col.InsertOne(sessionContext, interfaceData.Data)
					if err != nil {
						t.Errorf("main() error %v",err)
						u.ErrorLogger.Println("An Error Occured: Document Insertion Failed:",f.Name(),":",err)

					}else{
						err := os.Rename(configs.InputFolderPath+f.Name(), configs.OutputFolderPath+f.Name())
						if err != nil {
							u.GeneralLogger.Println(" Moving File to Output Folder:",f.Name(),":",err)
							t.Errorf("main() error %v",err)
						}
					}
				}else{
					u.ErrorLogger.Println("An Error Occured: SKIPPING INSERTION:",f.Name(),":",err)
					t.Errorf("main() error %v",err)
				}
			
				time.Sleep(time.Millisecond)

			}
			
			
			if err != nil {
				sessionContext.AbortTransaction(sessionContext)
				u.ErrorLogger.Println("An Error Occured: Transaction Aborted:",err)
				t.Errorf("main() error %v",err)
				return err
			} else{
				sessionContext.CommitTransaction(sessionContext)
				u.GeneralLogger.Println("Done !")
				
			}
			return nil
		})
	}else{
		os.Exit(1)
		u.ErrorLogger.Println("An Error Occured: Aborted:",err)
		t.Errorf("main() error %v",err)
	}





}
