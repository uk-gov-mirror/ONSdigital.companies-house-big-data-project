# $1 is the first argument to the script
# This is a directory to apply the script to or it can be a file
# Use directory/* to search subdirectories

# echo "===self.method() calls==="
# self_method_list=$(grep -r --include \*.py -E 'self\.[a-zA-Z0-9_\.]+\(' $1 | grep -Ev 'self\.fs|self\.__init__')
# echo "$self_method_list"

# echo "===Method Headers==="
# method_headers=$(grep -r --include \*.py 'def ' $1)
# echo "$method_headers"

#file1=($(grep -r --include \*.py 'def ' ~/companies-house-big-data-project/archive/src/*))
#file2=($(grep -r --include \*.py 'def ' ~/companies-house-big-data-project/src/*))
#echo ${file1[@]} ${file2[@]} | sort | uniq -u

#comm -12 $(grep -r --include \*.py 'def ' ~/companies-house-big-data-project/src/*) $(grep -r --include \*.py 'def ' ~/companies-house-big-data-project/archive/src/*)
#comm -12 $(grep -r --include \*.py 'def ' ~/companies-house-big-data-project/src/data_processing/xbrl_parser.py) $(grep -r --include \*.py 'def ' ~/companies-house-big-data-project/archive/src/data_processing/xbrl_parser.py)

#grep -F -f $(eval $file1) $(eval $file2)

#list1=($(grep -r --include \*.py 'def ' ~/companies-house-big-data-project/archive/src/* | awk -F '/' '{print $(NF)}' | sort))
#list2=($(grep -r --include \*.py 'def ' ~/companies-house-big-data-project/src/* | awk -F '/' '{print $(NF)}' | sort))

list1=$(grep -r --include \*.py 'def ' ~/companies-house-big-data-project/archive/src/* | awk -F '/' '{print $(NF)}' | sort)
list2=$(grep -r --include \*.py 'def ' ~/companies-house-big-data-project/src/* | awk -F '/' '{print $(NF)}' | sort)

grep -F -f <($list1) <($list2)

# #l2=" ${list2[*]} "                    # add framing blanks
# for item in ${list1[@]}; do
#   for itemb in ${list2[@]}; do    
#     if [[ $itemb =  ]] ; then    # use $item as regexp
#         result+=($item)
#     fi
#   done
# done
# echo  ${result[@]}

# echo "===Classes==="
# class_headers=$(grep -r --include \*.py -E 'class ' $1 | grep -Ev '"""' | grep -E ':$')
# echo "$class_headers"

# echo "===Class names==="
# #class_names=$(grep -r --include \*.py -E 'class ' $1 | grep -Ev '"""' | grep -E ':$' | awk '{print $2}' | awk -F '(' '{print $1}' | sed 's/://g')
# class_names=$(echo "$class_headers" | awk '{print $2}' | awk -F '(' '{print $1}' | sed 's/://g')
# echo "$class_names"

# class_names=($class_names)

# # ====================================

# echo "===Class static method calls==="
# for c in ${class_names[@]}; do

#     echo '---'$c'---'
#     instances=$(grep -rh --include \*.py -E "$c"'\.[a-zA-Z0-9_\.]+\(' $1 | sed 's/^[ \t]*//g' | grep -Ev 'import|^#' | awk -F " = " '{if (NF==1) print $1; else print $2;}')
#     echo "$instances"
# done

# # ================================

# echo "===Class instance method calls==="
# for c in ${class_names[@]}; do

#     echo '---'$c'---'
#     instances=$(grep -rh --include \*.py ' '"$c"'(' | sed 's/^[ \t]*//g' $1 | grep -Ev 'import|^#|^class|^return|^yield' | awk -F "=" '{print $1}')

#     echo 'instances: '"$instances"

#     instances=($instances)
#     for i in ${instances[@]}; do

#         echo '...'$i'...'
#         instance_calls=$(grep -r --include \*.py -E "$i"'\.[a-zA-Z0-9_\.]+\(' $1 | sed 's/^[ \t]*//g' | grep -Ev '^#' )
#         echo "$instance_calls"
#     done
# done