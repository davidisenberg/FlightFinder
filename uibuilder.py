
import os
import shutil
import glob
import subprocess
# ------
# first run:   node npm run-script build
# ------

# take the files and directory from the build directory and move them to the FlightFinder/static directory

def removeFilesInDir(folder):
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            print("removing: " + file_path)
            if os.path.isfile(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path): shutil.rmtree(file_path)
        except Exception as e:
            print(e)

def copyAllFilesinDir(srcDir, dstDir):
    # Check if both the are directories
    if os.path.isdir(srcDir) and os.path.isdir(dstDir) :
        # Iterate over all the files in source directory
        for file_path in glob.glob(srcDir + '\\*'):
            # Move each file to destination Directory
            if os.path.isfile(file_path):
                print("moving: " + file_path)
                shutil.copy(file_path, dstDir);
    else:
        print("srcDir & dstDir should be Directories")

def moveAllFilesinDir(srcDir, dstDir):
    # Check if both the are directories
    if os.path.isdir(srcDir) and os.path.isdir(dstDir) :
        # Iterate over all the files in source directory
        for filePath in glob.glob(srcDir + '\*'):
            # Move each file to destination Directory
            shutil.move(filePath, dstDir);
    else:
        print("srcDir & dstDir should be Directories")


if __name__ == "__main__":
    print("starting")
    high_level_path = "C:\\Users\\Dave\\PycharmProjects\\FlightFinder\\"
    ui_path = "C:\\Users\\Dave\\PycharmProjects\\FlightFinder\\ui"

    print("running npm")
    os.chdir(ui_path)
    subprocess.check_call('npm run-script build', shell=True)


    destination_folder = high_level_path + "\\static"
    source_folder = high_level_path + "\\ui\\build"
    js_folder = high_level_path + "\\ui\\build\\static\\js"
    css_folder = high_level_path + "\\ui\\build\\static\\css"
    media_folder = high_level_path + "\\ui\\build\\static\\media"
    removeFilesInDir(destination_folder)
    print("*removed existing static folder*")
    copyAllFilesinDir(source_folder,destination_folder)
    os.mkdir(destination_folder + "\\js")
    copyAllFilesinDir(js_folder, destination_folder + "\\js")
    os.mkdir(destination_folder + "\\css")
    copyAllFilesinDir(css_folder, destination_folder + "\\css")
    os.mkdir(destination_folder + "\\media")
    copyAllFilesinDir(media_folder, destination_folder + "\\media")
    print("*copied files into static folder*")

    print("complete")

    print("")
    print("Now, maybe you need to 'git add' the static folder. But commit, push, pull from remote, and restart the webserver.")




# take the folders from the static directory and move the up one level

