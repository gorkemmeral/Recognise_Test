from transform import Transform
    
def run_transformation():
    input_file = "/Users/gorkemmeral/Downloads/CC Application Lifecycle.csv"
    app_lifecycle = Transform(app_filepath=input_file)
    app_lifecycle.transform_data()


if __name__ == '__main__':
    run_transformation()