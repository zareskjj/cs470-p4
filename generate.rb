out = IO.sysopen("stress.txt", "w");
fd = IO.new(out, "w");
length = 16
entries = Hash.new()
p = -1
str = ""
for i in 1..512000 do
    fd.puts("proc " + (p+=1).to_s) if i % 16000 == 1
    loop do 
        str = rand(8**length).to_s(36)
        break if entries[str].nil?
    end
    entries[str] = 1
    fd.puts("  put " + str + " " + rand(1000).to_s)
end

out = IO.sysopen("stress_sync.txt", "w");
fd = IO.new(out, "w");
length = 8
entries = Hash.new()
p = -1
str = ""
for i in 1..64000 do
    fd.puts("proc " + (p+=1).to_s) if i % 16000 == 1
    fd.puts("  sync") if i % 16000 == 400
    loop do 
        str = rand(8**length).to_s(36)
        break if entries[str].nil?
    end
    entries[str] = 1
    fd.puts("  put " + str + " " + rand(10).to_s)
end
